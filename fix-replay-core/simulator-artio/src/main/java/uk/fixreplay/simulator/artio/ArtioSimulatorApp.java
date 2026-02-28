package uk.fixreplay.simulator.artio;

import io.fixreplay.simulator.artio.ArtioSimulator;
import io.fixreplay.simulator.artio.ArtioSimulatorConfig;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class ArtioSimulatorApp {
    private static final int EXIT_OK = 0;
    private static final int EXIT_CONFIG_ERROR = 3;
    private static final int EXIT_RUNTIME_ERROR = 4;

    private ArtioSimulatorApp() {
    }

    public static void main(String[] args) {
        int exitCode = run(args, System.out, System.err);
        System.exit(exitCode);
    }

    static int run(String[] args, PrintStream out, PrintStream err) {
        Objects.requireNonNull(args, "args");
        Objects.requireNonNull(out, "out");
        Objects.requireNonNull(err, "err");

        ParsedArgs parsedArgs;
        try {
            parsedArgs = ParsedArgs.parse(args);
        } catch (IllegalArgumentException parseFailure) {
            err.println(parseFailure.getMessage());
            printUsage(err);
            return EXIT_CONFIG_ERROR;
        }

        ArtioSimulatorConfig config;
        try {
            config = ArtioSimulatorConfig.load(parsedArgs.scenarioPath());
            config = config.withOverrides(
                parsedArgs.overrideEntryPort(),
                parsedArgs.overrideExitPort(),
                parsedArgs.rulesOverridePath()
            );
            if (!config.enabled()) {
                throw new IllegalArgumentException("simulator.enabled must be true to run ArtioSimulatorApp");
            }
            if (!"artio".equalsIgnoreCase(config.provider())) {
                throw new IllegalArgumentException(
                    "simulator.provider must be 'artio' for ArtioSimulatorApp, but was: " + config.provider()
                );
            }
        } catch (IOException | IllegalArgumentException configFailure) {
            err.println(configFailure.getMessage());
            return EXIT_CONFIG_ERROR;
        }

        AtomicReference<Throwable> runtimeFailure = new AtomicReference<>();
        AtomicBoolean shuttingDown = new AtomicBoolean(false);
        CountDownLatch stopSignal = new CountDownLatch(1);

        ArtioSimulator simulator;
        try {
            simulator = ArtioSimulator.start(config);
        } catch (RuntimeException startupFailure) {
            err.println("Failed to start Artio simulator: " + startupFailure.getMessage());
            return EXIT_RUNTIME_ERROR;
        }

        Runnable shutdownAction = () -> {
            if (!shuttingDown.compareAndSet(false, true)) {
                return;
            }
            try {
                simulator.stop();
            } catch (RuntimeException stopFailure) {
                runtimeFailure.compareAndSet(null, stopFailure);
            } finally {
                stopSignal.countDown();
            }
        };

        Thread shutdownHook = new Thread(shutdownAction, "artio-simulator-shutdown-hook");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        out.printf(
            "Artio simulator started at %s%nENTRY listen=%s:%d localCompId=%s remoteCompId=%s beginString=%s%n" +
                "EXIT  listen=%s:%d localCompId=%s remoteCompId=%s beginString=%s%n",
            Instant.now(),
            config.entry().listenHost(),
            config.entry().listenPort(),
            config.entry().localCompId(),
            config.entry().remoteCompId(),
            config.beginString(),
            config.exit().listenHost(),
            config.exit().listenPort(),
            config.exit().localCompId(),
            config.exit().remoteCompId(),
            config.beginString()
        );
        out.println("Press Ctrl+C to stop simulator.");

        int exitCode = EXIT_OK;
        try {
            while (!shuttingDown.get()) {
                if (stopSignal.await(2, TimeUnit.SECONDS)) {
                    break;
                }

                ArtioSimulator.Diagnostics diagnostics = simulator.diagnosticsSnapshot();
                out.printf(
                    "status ready=%s acquired=%d queueDepth=%d entry(acquired=%s connected=%s) " +
                        "exit(acquired=%s connected=%s)%n",
                    simulator.isReady(),
                    diagnostics.sessionsAcquired(),
                    diagnostics.queueDepth(),
                    diagnostics.entrySessionAcquired(),
                    diagnostics.entrySessionConnected(),
                    diagnostics.exitSessionAcquired(),
                    diagnostics.exitSessionConnected()
                );

                if (diagnostics.lastError() != null) {
                    err.println("Simulator runtime error: " + diagnostics.lastError());
                    runtimeFailure.compareAndSet(null, new IllegalStateException(diagnostics.lastError()));
                    shutdownAction.run();
                    break;
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            shutdownAction.run();
        } finally {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
                // JVM shutdown already in progress.
            }
        }

        Throwable failure = runtimeFailure.get();
        if (failure != null) {
            err.println("Simulator stopped with error: " + failure.getMessage());
            exitCode = EXIT_RUNTIME_ERROR;
        } else {
            out.println("Artio simulator stopped.");
        }

        return exitCode;
    }

    private static void printUsage(PrintStream stream) {
        stream.println("Usage: ArtioSimulatorApp --scenario <scenario.yaml> [--overrideEntryPort <port>]");
        stream.println("       [--overrideExitPort <port>] [--rules <path-to-rules-file>]");
    }

    private record ParsedArgs(
        Path scenarioPath,
        Integer overrideEntryPort,
        Integer overrideExitPort,
        Path rulesOverridePath
    ) {
        private static ParsedArgs parse(String[] args) {
            Path scenario = null;
            Integer entryPort = null;
            Integer exitPort = null;
            Path rules = null;

            int index = 0;
            while (index < args.length) {
                String arg = args[index];
                switch (arg) {
                    case "--scenario":
                        scenario = Path.of(requiredValue(args, ++index, "--scenario"));
                        index++;
                        break;
                    case "--overrideEntryPort":
                        entryPort = parsePort(requiredValue(args, ++index, "--overrideEntryPort"), "--overrideEntryPort");
                        index++;
                        break;
                    case "--overrideExitPort":
                        exitPort = parsePort(requiredValue(args, ++index, "--overrideExitPort"), "--overrideExitPort");
                        index++;
                        break;
                    case "--rules":
                        rules = Path.of(requiredValue(args, ++index, "--rules"));
                        index++;
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }

            if (scenario == null) {
                throw new IllegalArgumentException("Missing required argument: --scenario");
            }
            return new ParsedArgs(scenario, entryPort, exitPort, rules);
        }

        private static String requiredValue(String[] args, int index, String option) {
            if (index >= args.length) {
                throw new IllegalArgumentException("Missing value for " + option);
            }
            String value = args[index];
            if (value.startsWith("--")) {
                throw new IllegalArgumentException("Missing value for " + option);
            }
            return value;
        }

        private static int parsePort(String value, String option) {
            int port;
            try {
                port = Integer.parseInt(value);
            } catch (NumberFormatException invalidPort) {
                throw new IllegalArgumentException(option + " must be an integer: " + value);
            }
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException(option + " must be in range 1..65535");
            }
            return port;
        }
    }
}
