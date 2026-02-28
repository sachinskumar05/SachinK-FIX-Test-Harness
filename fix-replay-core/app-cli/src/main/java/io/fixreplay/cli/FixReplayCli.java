package io.fixreplay.cli;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.fixreplay.compare.CompareResult;
import io.fixreplay.compare.DiffReport;
import io.fixreplay.linker.LinkReport;
import io.fixreplay.loader.FixLogScanner;
import io.fixreplay.loader.FixRawMessage;
import io.fixreplay.model.FixMessage;
import io.fixreplay.runner.FixTransport;
import io.fixreplay.runner.OfflineRunResult;
import io.fixreplay.runner.OfflineRunner;
import io.fixreplay.runner.OnlineRunResult;
import io.fixreplay.runner.OnlineRunner;
import io.fixreplay.runner.OnlineRunnerConfig;
import io.fixreplay.runner.ScenarioConfig;
import io.fixreplay.runner.SessionKey;
import io.fixreplay.runner.TransportSessionConfig;
import io.fixreplay.simulator.artio.ArtioSimulator;
import io.fixreplay.simulator.artio.ArtioSimulatorConfig;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Option;

@Command(
    name = "fix-replay",
    mixinStandardHelpOptions = true,
    versionProvider = FixReplayCli.VersionProvider.class,
    description = "Run FIX replay workflows from command line",
    subcommands = {
        FixReplayCli.ScanCommand.class,
        FixReplayCli.PrepareCommand.class,
        FixReplayCli.RunOfflineCommand.class,
        FixReplayCli.RunOnlineCommand.class
    }
)
public final class FixReplayCli implements Callable<Integer> {
    private static final int EXIT_OK = 0;
    private static final int EXIT_COMPARE_FAIL = 2;
    private static final int EXIT_CONFIG_ERROR = 3;
    private static final String DEFAULT_ARTIO_TRANSPORT_CLASS = "io.fixreplay.adapter.artio.ArtioFixTransport";
    private static final long SIMULATOR_READY_POLL_INTERVAL_MS = 25L;
    private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    @Override
    public Integer call() {
        System.out.println("fix-replay-core version " + currentVersion());
        return EXIT_OK;
    }

    static String currentVersion() {
        String version = System.getProperty("fix.replay.version");
        return version == null || version.isBlank() ? "0.1.0-SNAPSHOT" : version;
    }

    static CommandLine newCommandLine() {
        CommandLine commandLine = new CommandLine(new FixReplayCli());
        commandLine.setParameterExceptionHandler((ex, args) -> {
            commandLine.getErr().println(ex.getMessage());
            return EXIT_CONFIG_ERROR;
        });
        commandLine.setExecutionExceptionHandler((ex, cmd, parseResult) -> {
            cmd.getErr().println(ex.getMessage());
            return EXIT_CONFIG_ERROR;
        });
        return commandLine;
    }

    public static void main(String[] args) {
        int exitCode = newCommandLine().execute(args);
        System.exit(exitCode);
    }

    @Command(
        name = "scan",
        mixinStandardHelpOptions = true,
        description = "Scan log file(s) and output message counts, msg types, and detected sessions as JSON"
    )
    static final class ScanCommand implements Callable<Integer> {
        @Option(names = "--path", required = true, description = "Log file or directory to scan")
        private Path path;

        @Option(names = "--out", required = true, description = "Output JSON file")
        private Path out;

        @Override
        public Integer call() {
            try {
                List<Path> files = collectInputFiles(path);
                FixLogScanner scanner = new FixLogScanner();

                long messageCount = 0L;
                Map<String, Integer> msgTypeDistribution = new TreeMap<>();
                Map<String, Integer> sessions = new TreeMap<>();

                for (Path file : files) {
                    try (Stream<FixRawMessage> messages = scanner.scan(file)) {
                        for (FixRawMessage raw : (Iterable<FixRawMessage>) messages::iterator) {
                            messageCount++;
                            FixMessage message = raw.toFixMessage();
                            msgTypeDistribution.merge(defaultString(message.msgType(), "UNKNOWN"), 1, Integer::sum);
                            sessions.merge(resolveSessionId(message), 1, Integer::sum);
                        }
                    }
                }

                Map<String, Object> report = new LinkedHashMap<>();
                report.put("path", normalizePath(path));
                report.put("filesScanned", files.size());
                report.put("messageCount", messageCount);
                report.put("msgTypeDistribution", msgTypeDistribution);
                report.put("sessionsDetected", toSessionCounts(sessions));

                writeJson(out, report);
                return EXIT_OK;
            } catch (IOException | IllegalArgumentException | UncheckedIOException scanFailure) {
                System.err.println(scanFailure.getMessage());
                return EXIT_CONFIG_ERROR;
            }
        }
    }

    @Command(
        name = "prepare",
        mixinStandardHelpOptions = true,
        description = "Prepare linking artifacts from input/expected folders and cache stable LinkReport JSON"
    )
    static final class PrepareCommand implements Callable<Integer> {
        @Option(names = "--in", required = true, description = "Input folder containing *.in files")
        private Path inputFolder;

        @Option(names = "--expected", required = true, description = "Expected folder containing *.out files")
        private Path expectedFolder;

        @Option(names = "--scenario", required = true, description = "Scenario YAML/JSON used as config template")
        private Path scenarioPath;

        @Option(names = "--cache", required = true, description = "Directory to write cache artifacts")
        private Path cacheDir;

        @Override
        public Integer call() {
            try {
                ScenarioConfig baseConfig = ScenarioConfig.load(scenarioPath);
                ScenarioConfig effectiveConfig = copyScenarioWithFolders(baseConfig, inputFolder, expectedFolder, null);

                OfflineRunResult result = new OfflineRunner().run(effectiveConfig);
                Map<String, JsonNode> linkReports = parseAndSortLinkReports(result.linkReports());

                int matched = 0;
                int unmatched = 0;
                int ambiguous = 0;
                for (LinkReport report : result.linkReports().values()) {
                    matched += report.matched();
                    unmatched += report.unmatched();
                    ambiguous += report.ambiguous();
                }

                Map<String, Object> counts = new LinkedHashMap<>();
                counts.put("sessions", linkReports.size());
                counts.put("matched", matched);
                counts.put("unmatched", unmatched);
                counts.put("ambiguous", ambiguous);

                Map<String, Object> output = new LinkedHashMap<>();
                output.put("scenario", normalizePath(scenarioPath));
                output.put("inputFolder", normalizePath(inputFolder));
                output.put("expectedFolder", normalizePath(expectedFolder));
                output.put("counts", counts);
                output.put("linkReports", linkReports);

                Files.createDirectories(cacheDir);
                writeJson(cacheDir.resolve("link-report.json"), output);
                for (Map.Entry<String, JsonNode> entry : linkReports.entrySet()) {
                    writeJson(cacheDir.resolve("link-report-" + sanitizeFilePart(entry.getKey()) + ".json"), entry.getValue());
                }

                printJson(output);
                return EXIT_OK;
            } catch (IOException | IllegalArgumentException prepareFailure) {
                System.err.println(prepareFailure.getMessage());
                return EXIT_CONFIG_ERROR;
            }
        }
    }

    @Command(
        name = "run-offline",
        mixinStandardHelpOptions = true,
        description = "Run offline compare using scenario config and emit DiffReport JSON"
    )
    static final class RunOfflineCommand implements Callable<Integer> {
        @Option(names = "--scenario", required = true, description = "Scenario YAML/JSON")
        private Path scenarioPath;

        @Option(names = "--out", description = "Write report JSON to this file (default: stdout)")
        private Path out;

        @Option(names = "--junit", description = "Optional JUnit XML output path")
        private Path junitOut;

        @Override
        public Integer call() {
            try {
                ScenarioConfig scenarioConfig = ScenarioConfig.load(scenarioPath);
                OfflineRunResult result = new OfflineRunner().run(scenarioConfig);

                DiffReport effectiveDiff = withOperationalFailures(
                    result.diffReport(),
                    "offline",
                    result.unmatchedExpected(),
                    result.unmatchedActual(),
                    result.ambiguous(),
                    false,
                    0
                );
                Map<String, Object> report = buildOfflineOutput(result, effectiveDiff);

                if (out != null) {
                    writeJson(out, report);
                } else {
                    printJson(report);
                }
                if (junitOut != null) {
                    writeText(junitOut, effectiveDiff.toJUnitXml("run-offline"));
                }
                return result.passed() ? EXIT_OK : EXIT_COMPARE_FAIL;
            } catch (IOException | IllegalArgumentException runFailure) {
                System.err.println(runFailure.getMessage());
                return EXIT_CONFIG_ERROR;
            }
        }
    }

    @Command(
        name = "run-online",
        mixinStandardHelpOptions = true,
        description = "Run online replay against a transport implementation and emit DiffReport JSON + JUnit XML"
    )
    static final class RunOnlineCommand implements Callable<Integer> {
        @Option(names = "--scenario", required = true, description = "Scenario YAML/JSON")
        private Path scenarioPath;

        @Option(
            names = "--transport-class",
            description = "Fully qualified FixTransport implementation class (optional when --start-simulator uses Artio)"
        )
        private String transportClassName;

        @Option(
            names = "--transport-prop",
            description = "Transport key=value pair; repeatable",
            paramLabel = "KEY=VALUE"
        )
        private Map<String, String> transportProperties = new LinkedHashMap<>();

        @Option(names = "--transport-config", description = "Optional transport properties file (.json or .properties)")
        private Path transportConfigFile;

        @Option(names = "--receive-timeout-ms", defaultValue = "5000", description = "Receive timeout in milliseconds")
        private long receiveTimeoutMs;

        @Option(names = "--queue-capacity", defaultValue = "1024", description = "Online receive queue capacity")
        private int queueCapacity;

        @Option(names = "--out", description = "Write report JSON to this file (default: stdout)")
        private Path out;

        @Option(names = "--junit", defaultValue = "junit.xml", description = "JUnit XML output path")
        private Path junitOut;

        @Option(
            names = "--start-simulator",
            defaultValue = "false",
            description = "Start embedded Artio simulator when scenario.simulator is enabled with provider=artio"
        )
        private boolean startSimulator;

        @Option(
            names = "--simulator-ready-timeout-ms",
            defaultValue = "15000",
            description = "Timeout waiting for embedded simulator readiness"
        )
        private long simulatorReadyTimeoutMs;

        @Override
        public Integer call() {
            EmbeddedSimulatorHandle simulatorHandle = EmbeddedSimulatorHandle.noop(Map.of());
            try {
                if (receiveTimeoutMs <= 0) {
                    throw new IllegalArgumentException("--receive-timeout-ms must be > 0");
                }
                if (queueCapacity <= 0) {
                    throw new IllegalArgumentException("--queue-capacity must be > 0");
                }
                if (startSimulator && simulatorReadyTimeoutMs <= 0) {
                    throw new IllegalArgumentException("--simulator-ready-timeout-ms must be > 0 when --start-simulator is set");
                }

                ScenarioConfig scenarioConfig = ScenarioConfig.load(scenarioPath);
                String effectiveTransportClassName = resolveTransportClassName(transportClassName, startSimulator, scenarioConfig);
                Map<String, String> mergedTransportProperties = mergeTransportProperties(transportConfigFile, transportProperties);
                mergedTransportProperties = applyScenarioInitiatorTransportDefaults(scenarioConfig, mergedTransportProperties);
                simulatorHandle = maybeStartSimulator(
                    scenarioConfig,
                    mergedTransportProperties,
                    startSimulator,
                    simulatorReadyTimeoutMs
                );
                Map<String, String> effectiveTransportProperties = simulatorHandle.transportProperties();

                Map<SessionKey, Path> inputFiles = scenarioConfig.resolveInputFiles();
                Map<SessionKey, Path> expectedFiles = scenarioConfig.resolveExpectedFiles();
                List<SessionKey> sessions = new ArrayList<>(expectedFiles.keySet());
                sessions.sort(Comparator.comparing(SessionKey::id));

                if (sessions.isEmpty()) {
                    throw new IllegalArgumentException("Scenario has no expected .out files");
                }

                FixLogScanner scanner = new FixLogScanner();
                List<DiffReport.MessageResult> allDiffMessages = new ArrayList<>();
                Map<String, JsonNode> linkReports = new TreeMap<>();
                List<Map<String, Object>> perSession = new ArrayList<>();

                int totalSent = 0;
                int totalReceived = 0;
                int totalDropped = 0;
                int totalMatched = 0;
                int totalUnmatchedExpected = 0;
                int totalUnmatchedActual = 0;
                int totalAmbiguous = 0;
                int timedOutSessions = 0;
                boolean overallPass = true;

                OnlineRunner runner = new OnlineRunner();
                for (SessionKey session : sessions) {
                    Path inputPath = inputFiles.get(session);
                    Path expectedPath = expectedFiles.get(session);
                    if (inputPath == null) {
                        throw new IllegalArgumentException("Missing input .in file for session " + session.id());
                    }

                    List<FixMessage> entryMessages = loadMessages(scanner, inputPath, scenarioConfig.msgTypeFilter());
                    List<FixMessage> expectedMessages = loadMessages(scanner, expectedPath, scenarioConfig.msgTypeFilter());

                    FixTransport transport = instantiateTransport(effectiveTransportClassName);
                    TransportSessionConfig transportSessionConfig = new TransportSessionConfig(
                        session,
                        reverseSession(session),
                        effectiveTransportProperties
                    );
                    OnlineRunnerConfig runnerConfig = new OnlineRunnerConfig(
                        scenarioConfig.linkerConfig(),
                        scenarioConfig.compareConfig(),
                        scenarioConfig.msgTypeFilter(),
                        Duration.ofMillis(receiveTimeoutMs),
                        queueCapacity
                    );

                    OnlineRunResult result = runner.run(
                        entryMessages,
                        expectedMessages,
                        transport,
                        transportSessionConfig,
                        runnerConfig
                    );

                    totalSent += result.sentCount();
                    totalReceived += result.receivedCount();
                    totalDropped += result.droppedCount();
                    totalMatched += result.matchedComparisons();
                    totalUnmatchedExpected += result.unmatchedExpected();
                    totalUnmatchedActual += result.unmatchedActual();
                    totalAmbiguous += result.ambiguous();
                    if (result.timedOut()) {
                        timedOutSessions++;
                    }

                    DiffReport sessionDiff = withOperationalFailures(
                        result.diffReport(),
                        session.id(),
                        result.unmatchedExpected(),
                        result.unmatchedActual(),
                        result.ambiguous(),
                        result.timedOut(),
                        result.droppedCount()
                    );
                    for (DiffReport.MessageResult message : sessionDiff.messages()) {
                        allDiffMessages.add(new DiffReport.MessageResult(session.id() + ":" + message.id(), message.result()));
                    }
                    linkReports.put(session.id(), parseJson(result.linkReport().toJson()));

                    boolean sessionPass = result.passed() && !result.timedOut();
                    overallPass = overallPass && sessionPass;

                    Map<String, Object> sessionSummary = new LinkedHashMap<>();
                    sessionSummary.put("session", session.id());
                    sessionSummary.put("sent", result.sentCount());
                    sessionSummary.put("received", result.receivedCount());
                    sessionSummary.put("dropped", result.droppedCount());
                    sessionSummary.put("timedOut", result.timedOut());
                    sessionSummary.put("matchedComparisons", result.matchedComparisons());
                    sessionSummary.put("unmatchedExpected", result.unmatchedExpected());
                    sessionSummary.put("unmatchedActual", result.unmatchedActual());
                    sessionSummary.put("ambiguous", result.ambiguous());
                    sessionSummary.put("passed", sessionPass);
                    perSession.add(sessionSummary);
                }

                DiffReport diffReport = new DiffReport(allDiffMessages);
                Map<String, Object> counts = new LinkedHashMap<>();
                counts.put("sessions", sessions.size());
                counts.put("timedOutSessions", timedOutSessions);
                counts.put("sent", totalSent);
                counts.put("received", totalReceived);
                counts.put("dropped", totalDropped);
                counts.put("matchedComparisons", totalMatched);
                counts.put("unmatchedExpected", totalUnmatchedExpected);
                counts.put("unmatchedActual", totalUnmatchedActual);
                counts.put("ambiguous", totalAmbiguous);

                Map<String, Object> report = new LinkedHashMap<>();
                report.put("passed", overallPass);
                report.put("scenario", normalizePath(scenarioPath));
                report.put("counts", counts);
                report.put("sessions", perSession);
                report.put("diffReport", parseJson(diffReport.toJson()));
                report.put("linkReports", linkReports);

                if (out != null) {
                    writeJson(out, report);
                } else {
                    printJson(report);
                }
                writeText(junitOut, diffReport.toJUnitXml("run-online"));

                return overallPass ? EXIT_OK : EXIT_COMPARE_FAIL;
            } catch (IOException | ReflectiveOperationException | IllegalArgumentException runFailure) {
                System.err.println(runFailure.getMessage());
                return EXIT_CONFIG_ERROR;
            } finally {
                try {
                    simulatorHandle.close();
                } catch (RuntimeException stopFailure) {
                    System.err.println("Failed to stop embedded simulator: " + stopFailure.getMessage());
                }
            }
        }
    }

    public static final class VersionProvider implements IVersionProvider {
        @Override
        public String[] getVersion() {
            return new String[] {"fix-replay-core " + currentVersion()};
        }
    }

    private static Map<String, Object> buildOfflineOutput(OfflineRunResult result, DiffReport effectiveDiff) throws IOException {
        Map<String, Object> counts = new LinkedHashMap<>();
        counts.put("matchedComparisons", result.matchedComparisons());
        counts.put("unmatchedExpected", result.unmatchedExpected());
        counts.put("unmatchedActual", result.unmatchedActual());
        counts.put("ambiguous", result.ambiguous());

        Map<String, Object> output = new LinkedHashMap<>();
        output.put("passed", result.passed());
        output.put("usedActualMessages", result.usedActualMessages());
        output.put("counts", counts);
        output.put("diffReport", parseJson(effectiveDiff.toJson()));
        output.put("linkReports", parseAndSortLinkReports(result.linkReports()));
        return output;
    }

    private static Map<String, JsonNode> parseAndSortLinkReports(Map<SessionKey, LinkReport> source) throws IOException {
        Map<String, JsonNode> sorted = new TreeMap<>();
        for (Map.Entry<SessionKey, LinkReport> entry : source.entrySet()) {
            sorted.put(entry.getKey().id(), parseJson(entry.getValue().toJson()));
        }
        return sorted;
    }

    private static DiffReport withOperationalFailures(
        DiffReport base,
        String scope,
        int unmatchedExpected,
        int unmatchedActual,
        int ambiguous,
        boolean timedOut,
        int dropped
    ) {
        if (
            unmatchedExpected <= 0 &&
            unmatchedActual <= 0 &&
            ambiguous <= 0 &&
            dropped <= 0 &&
            !timedOut
        ) {
            return base;
        }

        List<DiffReport.MessageResult> messages = new ArrayList<>(base.messages());
        if (unmatchedExpected > 0) {
            messages.add(operationalFailure(scope + ":unmatched-expected", "unmatchedExpected", unmatchedExpected));
        }
        if (unmatchedActual > 0) {
            messages.add(operationalFailure(scope + ":unmatched-actual", "unmatchedActual", unmatchedActual));
        }
        if (ambiguous > 0) {
            messages.add(operationalFailure(scope + ":ambiguous", "ambiguous", ambiguous));
        }
        if (dropped > 0) {
            messages.add(operationalFailure(scope + ":dropped", "dropped", dropped));
        }
        if (timedOut) {
            messages.add(operationalFailure(scope + ":timeout", "timedOut", 1));
        }
        return new DiffReport(messages);
    }

    private static DiffReport.MessageResult operationalFailure(String id, String field, int value) {
        CompareResult failure = new CompareResult(
            "SYSTEM",
            List.of(),
            List.of(),
            Map.of(9000, new CompareResult.ValueDifference(field + "=0", field + "=" + value))
        );
        return new DiffReport.MessageResult(id, failure);
    }

    private static Map<String, String> mergeTransportProperties(Path configFile, Map<String, String> commandLineProperties)
        throws IOException {
        Map<String, String> merged = new LinkedHashMap<>();
        if (configFile != null) {
            merged.putAll(loadTransportConfig(configFile));
        }
        merged.putAll(commandLineProperties);
        return Map.copyOf(merged);
    }

    static String resolveTransportClassName(
        String requestedTransportClassName,
        boolean startSimulator,
        ScenarioConfig scenarioConfig
    ) {
        if (requestedTransportClassName != null && !requestedTransportClassName.isBlank()) {
            return requestedTransportClassName.trim();
        }
        if (
            startSimulator &&
            scenarioConfig.simulator().enabled() &&
            "artio".equalsIgnoreCase(defaultString(scenarioConfig.simulator().provider(), "none"))
        ) {
            return DEFAULT_ARTIO_TRANSPORT_CLASS;
        }
        throw new IllegalArgumentException(
            "Missing required argument: --transport-class (or use --start-simulator with scenario.simulator.provider=artio)"
        );
    }

    static Map<String, String> applySimulatorTransportDefaults(
        Map<String, String> transportProperties,
        ArtioSimulatorConfig simulatorConfig
    ) {
        Objects.requireNonNull(transportProperties, "transportProperties");
        Objects.requireNonNull(simulatorConfig, "simulatorConfig");

        Map<String, String> merged = new LinkedHashMap<>(transportProperties);
        merged.putIfAbsent("artio.host", connectHostFromListenHost(simulatorConfig.entry().listenHost()));
        merged.putIfAbsent("artio.port", Integer.toString(simulatorConfig.entry().listenPort()));
        merged.putIfAbsent("artio.exitHost", connectHostFromListenHost(simulatorConfig.exit().listenHost()));
        merged.putIfAbsent("artio.exitPort", Integer.toString(simulatorConfig.exit().listenPort()));
        return Map.copyOf(merged);
    }

    static Map<String, String> applyScenarioInitiatorTransportDefaults(
        ScenarioConfig scenarioConfig,
        Map<String, String> transportProperties
    ) {
        Objects.requireNonNull(scenarioConfig, "scenarioConfig");
        Objects.requireNonNull(transportProperties, "transportProperties");

        ScenarioConfig.SessionIdentity entry = scenarioConfig.sessions().entry();
        ScenarioConfig.SessionIdentity exit = scenarioConfig.sessions().exit();

        Map<String, String> merged = new LinkedHashMap<>(transportProperties);
        putIfNonBlankAbsent(merged, "artio.host", entry.host());
        putIfPositivePortAbsent(merged, "artio.port", entry.port());
        putIfNonBlankAbsent(merged, "artio.exitHost", exit.host());
        putIfPositivePortAbsent(merged, "artio.exitPort", exit.port());
        return Map.copyOf(merged);
    }

    private static void putIfNonBlankAbsent(Map<String, String> target, String key, String value) {
        if (value != null && !value.isBlank()) {
            target.putIfAbsent(key, value.trim());
        }
    }

    private static void putIfPositivePortAbsent(Map<String, String> target, String key, Integer value) {
        if (value != null && value > 0) {
            target.putIfAbsent(key, Integer.toString(value));
        }
    }

    private static EmbeddedSimulatorHandle maybeStartSimulator(
        ScenarioConfig scenarioConfig,
        Map<String, String> transportProperties,
        boolean startSimulator,
        long readyTimeoutMs
    ) {
        if (!startSimulator) {
            return EmbeddedSimulatorHandle.noop(transportProperties);
        }

        ScenarioConfig.Simulator simulatorSpec = scenarioConfig.simulator();
        if (!simulatorSpec.enabled()) {
            return EmbeddedSimulatorHandle.noop(transportProperties);
        }

        String provider = simulatorSpec.provider() == null ? "none" : simulatorSpec.provider().trim();
        if (!"artio".equalsIgnoreCase(provider)) {
            return EmbeddedSimulatorHandle.noop(transportProperties);
        }

        ArtioSimulatorConfig simulatorConfig;
        try {
            simulatorConfig = ArtioSimulatorConfig.fromScenario(scenarioConfig);
        } catch (RuntimeException configFailure) {
            throw new IllegalArgumentException("Invalid simulator configuration: " + configFailure.getMessage(), configFailure);
        }

        ArtioSimulator simulator;
        try {
            simulator = ArtioSimulator.start(simulatorConfig);
        } catch (RuntimeException startupFailure) {
            throw new IllegalArgumentException(
                "Failed to start embedded Artio simulator: " + startupFailure.getMessage(),
                startupFailure
            );
        }

        try {
            awaitSimulatorReady(simulator, readyTimeoutMs);
        } catch (RuntimeException readinessFailure) {
            try {
                simulator.stop();
            } catch (RuntimeException stopFailure) {
                readinessFailure.addSuppressed(stopFailure);
            }
            throw readinessFailure;
        }

        Map<String, String> effectiveProperties = applySimulatorTransportDefaults(transportProperties, simulatorConfig);
        return new EmbeddedSimulatorHandle(simulator, effectiveProperties);
    }

    private static void awaitSimulatorReady(ArtioSimulator simulator, long timeoutMs) {
        long deadlineNanos = System.nanoTime() + Duration.ofMillis(timeoutMs).toNanos();
        while (System.nanoTime() < deadlineNanos) {
            if (simulator.isReady()) {
                return;
            }
            ArtioSimulator.Diagnostics diagnostics = simulator.diagnosticsSnapshot();
            if (diagnostics.lastError() != null) {
                throw new IllegalArgumentException("Embedded simulator failed during startup: " + diagnostics.lastError());
            }
            try {
                Thread.sleep(SIMULATOR_READY_POLL_INTERVAL_MS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalArgumentException("Interrupted while waiting for embedded simulator readiness", interrupted);
            }
        }
        ArtioSimulator.Diagnostics diagnostics = simulator.diagnosticsSnapshot();
        throw new IllegalArgumentException(
            "Timed out waiting for embedded simulator readiness after " +
                timeoutMs +
                "ms (entryAcquired=" +
                diagnostics.entrySessionAcquired() +
                ", exitAcquired=" +
                diagnostics.exitSessionAcquired() +
                ", entryConnected=" +
                diagnostics.entrySessionConnected() +
                ", exitConnected=" +
                diagnostics.exitSessionConnected() +
                ")"
        );
    }

    private static String connectHostFromListenHost(String listenHost) {
        if (listenHost == null || listenHost.isBlank()) {
            return "localhost";
        }
        String host = listenHost.trim();
        if ("0.0.0.0".equals(host) || "::".equals(host) || "[::]".equals(host)) {
            return "127.0.0.1";
        }
        return host;
    }

    private static Map<String, String> loadTransportConfig(Path configFile) throws IOException {
        if (!Files.isRegularFile(configFile)) {
            throw new IllegalArgumentException("Transport config file does not exist: " + configFile);
        }
        String lower = configFile.getFileName().toString().toLowerCase(Locale.ROOT);
        if (lower.endsWith(".json")) {
            try (InputStream in = Files.newInputStream(configFile)) {
                Map<String, String> parsed = JSON.readValue(in, new TypeReference<>() {});
                return Map.copyOf(parsed);
            }
        }
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(configFile)) {
            properties.load(in);
        }
        Map<String, String> parsed = new LinkedHashMap<>();
        for (String propertyName : properties.stringPropertyNames()) {
            parsed.put(propertyName, properties.getProperty(propertyName));
        }
        return Map.copyOf(parsed);
    }

    private static ScenarioConfig copyScenarioWithFolders(
        ScenarioConfig base,
        Path inputFolder,
        Path expectedFolder,
        Path actualFolder
    ) {
        ScenarioConfig.Builder builder = ScenarioConfig.builder()
            .inputFolder(inputFolder)
            .expectedFolder(expectedFolder)
            .msgTypeFilter(base.msgTypeFilter())
            .linkerConfig(base.linkerConfig())
            .compareConfig(base.compareConfig())
            .sessionMappingRules(base.sessionMappingRules())
            .sessions(base.sessions())
            .simulator(base.simulator())
            .scenarioBaseDirectory(base.scenarioBaseDirectory());
        if (base.hasCacheFolder()) {
            builder.cacheFolder(base.cacheFolder());
        }
        if (actualFolder != null) {
            builder.actualFolder(actualFolder);
        }
        return builder.build();
    }

    private static List<FixMessage> loadMessages(FixLogScanner scanner, Path path, io.fixreplay.model.MsgTypeFilter filter) {
        if (path == null) {
            return List.of();
        }
        try (Stream<FixRawMessage> messages = scanner.scan(path)) {
            return messages
                .map(FixRawMessage::toFixMessage)
                .filter(filter::accepts)
                .toList();
        }
    }

    private static List<Map<String, Object>> toSessionCounts(Map<String, Integer> sessions) {
        List<Map<String, Object>> result = new ArrayList<>(sessions.size());
        for (Map.Entry<String, Integer> entry : sessions.entrySet()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("session", entry.getKey());
            item.put("count", entry.getValue());
            result.add(item);
        }
        return result;
    }

    private static List<Path> collectInputFiles(Path path) throws IOException {
        if (Files.isRegularFile(path)) {
            return List.of(path);
        }
        if (!Files.isDirectory(path)) {
            throw new IllegalArgumentException("Input path does not exist or is not a file/directory: " + path);
        }
        try (Stream<Path> stream = Files.walk(path)) {
            return stream
                .filter(Files::isRegularFile)
                .sorted(Comparator.comparing(Path::toString))
                .toList();
        }
    }

    private static String normalizePath(Path path) {
        return path.toAbsolutePath().normalize().toString();
    }

    private static String resolveSessionId(FixMessage message) {
        String sender = message.senderCompId();
        String target = message.targetCompId();
        if (sender == null || target == null || sender.isBlank() || target.isBlank()) {
            return "UNKNOWN";
        }
        return sender + "_" + target;
    }

    private static String defaultString(String value, String fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return value;
    }

    private static String sanitizeFilePart(String value) {
        return value.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    private static SessionKey reverseSession(SessionKey session) {
        return new SessionKey(session.targetCompId(), session.senderCompId());
    }

    private static FixTransport instantiateTransport(String className) throws ReflectiveOperationException {
        Objects.requireNonNull(className, "className");
        Class<?> type = Class.forName(className);
        if (!FixTransport.class.isAssignableFrom(type)) {
            throw new IllegalArgumentException(className + " does not implement " + FixTransport.class.getName());
        }
        Object instance = type.getDeclaredConstructor().newInstance();
        return (FixTransport) instance;
    }

    private static JsonNode parseJson(String json) throws IOException {
        return JSON.readTree(json);
    }

    private static void printJson(Object value) throws IOException {
        System.out.println(JSON.writeValueAsString(value));
    }

    private static void writeJson(Path out, Object value) throws IOException {
        Path parent = out.toAbsolutePath().normalize().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        JSON.writeValue(out.toFile(), value);
    }

    private static void writeText(Path out, String content) throws IOException {
        Path parent = out.toAbsolutePath().normalize().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(out, content);
    }

    private static final class EmbeddedSimulatorHandle implements AutoCloseable {
        private final ArtioSimulator simulator;
        private final Map<String, String> transportProperties;

        private EmbeddedSimulatorHandle(ArtioSimulator simulator, Map<String, String> transportProperties) {
            this.simulator = simulator;
            this.transportProperties = Map.copyOf(transportProperties);
        }

        static EmbeddedSimulatorHandle noop(Map<String, String> transportProperties) {
            return new EmbeddedSimulatorHandle(null, transportProperties);
        }

        Map<String, String> transportProperties() {
            return transportProperties;
        }

        @Override
        public void close() {
            if (simulator != null) {
                simulator.stop();
            }
        }
    }
}
