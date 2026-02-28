package io.fixreplay.simulator.artio;

import io.aeron.CommonContext;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.OnMessageInfo;
import uk.co.real_logic.artio.library.SessionAcquiredInfo;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner;
import uk.co.real_logic.artio.session.CompositeKey;
import uk.co.real_logic.artio.session.Session;

public final class ArtioSimulator implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArtioSimulator.class);
    private static final long STARTUP_TIMEOUT_MS = 15_000L;
    private static final long POLL_IDLE_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    private final ArtioSimulatorConfig config;
    private final EndpointRuntime entryRuntime;
    private final EndpointRuntime exitRuntime;
    private final AtomicInteger queueDepth = new AtomicInteger();
    private final AtomicReference<Throwable> lastError = new AtomicReference<>();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    private ArtioSimulator(ArtioSimulatorConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.entryRuntime = new EndpointRuntime("entry", config.entry());
        this.exitRuntime = new EndpointRuntime("exit", config.exit());
    }

    public static ArtioSimulator start(ArtioSimulatorConfig config) {
        ArtioSimulator simulator = new ArtioSimulator(config);
        simulator.startInternal();
        return simulator;
    }

    public int entryPort() {
        return config.entry().listenPort();
    }

    public int exitPort() {
        return config.exit().listenPort();
    }

    public boolean isReady() {
        if (lastError.get() != null) {
            return false;
        }
        return entryRuntime.isSessionReady() && exitRuntime.isSessionReady();
    }

    public Diagnostics diagnosticsSnapshot() {
        Throwable error = lastError.get();
        int sessionsAcquired = 0;
        if (entryRuntime.sessionAcquired()) {
            sessionsAcquired++;
        }
        if (exitRuntime.sessionAcquired()) {
            sessionsAcquired++;
        }

        return new Diagnostics(
            entryRuntime.sessionAcquired(),
            exitRuntime.sessionAcquired(),
            entryRuntime.sessionConnected(),
            exitRuntime.sessionConnected(),
            sessionsAcquired,
            queueDepth.get(),
            error == null ? null : error.toString()
        );
    }

    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }

        RuntimeException shutdownFailure = null;
        try {
            exitRuntime.stop();
        } catch (RuntimeException failure) {
            shutdownFailure = failure;
        }

        try {
            entryRuntime.stop();
        } catch (RuntimeException failure) {
            if (shutdownFailure == null) {
                shutdownFailure = failure;
            } else {
                shutdownFailure.addSuppressed(failure);
            }
        }

        if (config.storageDirs().cleanupOnStop()) {
            cleanupDirectory(config.storageDirs().logDir());
            cleanupDirectory(config.storageDirs().aeronDir());
            cleanupDirectory(config.storageDirs().workDir());
        }

        if (shutdownFailure != null) {
            throw shutdownFailure;
        }
    }

    @Override
    public void close() {
        stop();
    }

    private void startInternal() {
        if (!config.enabled()) {
            throw new IllegalStateException("Simulator is disabled in config");
        }
        if (!"artio".equalsIgnoreCase(config.provider())) {
            throw new IllegalStateException("Unsupported simulator provider for Artio module: " + config.provider());
        }
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("ArtioSimulator is already started");
        }

        try {
            entryRuntime.start(endpointPath("entry"));
            exitRuntime.start(endpointPath("exit"));
        } catch (RuntimeException startFailure) {
            lastError.compareAndSet(null, startFailure);
            try {
                stop();
            } catch (RuntimeException stopFailure) {
                startFailure.addSuppressed(stopFailure);
            }
            throw startFailure;
        }
    }

    private EndpointPaths endpointPath(String endpointName) {
        return new EndpointPaths(
            config.storageDirs().workDir().resolve(endpointName),
            config.storageDirs().aeronDir().resolve(endpointName),
            config.storageDirs().logDir().resolve(endpointName)
        );
    }

    private void cleanupDirectory(Path path) {
        if (path == null || !Files.exists(path)) {
            return;
        }
        try (var walk = Files.walk(path)) {
            walk.sorted((left, right) -> right.getNameCount() - left.getNameCount()).forEach(target -> {
                try {
                    Files.deleteIfExists(target);
                } catch (IOException deleteFailure) {
                    LOGGER.warn("Unable to delete simulator path {}", target, deleteFailure);
                }
            });
        } catch (IOException scanFailure) {
            LOGGER.warn("Unable to walk simulator path {}", path, scanFailure);
        }
    }

    private final class EndpointRuntime {
        private final String name;
        private final ArtioSimulatorConfig.SessionEndpoint endpoint;
        private final AtomicReference<Session> sessionRef = new AtomicReference<>();
        private final AtomicBoolean acquired = new AtomicBoolean();

        private FixEngine engine;
        private FixLibrary library;
        private MediaDriver mediaDriver;
        private Thread pollThread;
        private volatile boolean running;

        private EndpointRuntime(String name, ArtioSimulatorConfig.SessionEndpoint endpoint) {
            this.name = name;
            this.endpoint = endpoint;
        }

        private void start(EndpointPaths paths) {
            ensureDirectory(paths.workDir());
            ensureDirectory(paths.aeronDir());
            ensureDirectory(paths.logDir());

            EngineConfiguration engineConfiguration = buildEngineConfiguration(endpoint, paths);
            LibraryConfiguration libraryConfiguration = buildLibraryConfiguration(paths);

            MediaDriver launchedDriver = launchMediaDriver(paths.aeronDir());
            FixEngine launchedEngine = null;
            FixLibrary connectedLibrary = null;
            try {
                launchedEngine = FixEngine.launch(engineConfiguration);
                connectedLibrary = FixLibrary.connect(libraryConfiguration);
                awaitLibraryConnected(connectedLibrary);
                startPolling(connectedLibrary, config.performance().inboundFragmentLimit());
            } catch (RuntimeException failure) {
                if (connectedLibrary != null) {
                    try {
                        connectedLibrary.close();
                    } catch (RuntimeException closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
                if (launchedEngine != null) {
                    try {
                        launchedEngine.close();
                    } catch (RuntimeException closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
                try {
                    launchedDriver.close();
                } catch (RuntimeException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }

            this.mediaDriver = launchedDriver;
            this.engine = launchedEngine;
            this.library = connectedLibrary;
            LOGGER.info(
                "Artio simulator endpoint '{}' listening on {}:{} [localCompId={}, remoteCompId={}]",
                name,
                endpoint.listenHost(),
                endpoint.listenPort(),
                endpoint.localCompId(),
                endpoint.remoteCompId()
            );
        }

        private void stop() {
            running = false;
            Thread thread = pollThread;
            pollThread = null;
            if (thread != null && thread != Thread.currentThread()) {
                thread.interrupt();
                try {
                    thread.join(config.shutdown().gracefulTimeoutMs());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }

            RuntimeException failure = null;
            FixLibrary libraryToClose = library;
            library = null;
            if (libraryToClose != null) {
                try {
                    libraryToClose.close();
                } catch (RuntimeException closeFailure) {
                    failure = closeFailure;
                }
            }

            FixEngine engineToClose = engine;
            engine = null;
            if (engineToClose != null) {
                try {
                    engineToClose.close();
                } catch (RuntimeException closeFailure) {
                    if (failure == null) {
                        failure = closeFailure;
                    } else {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }

            MediaDriver driverToClose = mediaDriver;
            mediaDriver = null;
            if (driverToClose != null) {
                try {
                    driverToClose.close();
                } catch (RuntimeException closeFailure) {
                    if (failure == null) {
                        failure = closeFailure;
                    } else {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }

            sessionRef.set(null);
            acquired.set(false);

            if (failure != null) {
                throw failure;
            }
        }

        private boolean sessionAcquired() {
            return acquired.get();
        }

        private boolean sessionConnected() {
            Session session = sessionRef.get();
            return session != null && session.isConnected();
        }

        private boolean isSessionReady() {
            return sessionAcquired() && sessionConnected();
        }

        private EngineConfiguration buildEngineConfiguration(
            ArtioSimulatorConfig.SessionEndpoint endpoint,
            EndpointPaths paths
        ) {
            EngineConfiguration configuration = new EngineConfiguration()
                .bindTo(endpoint.listenHost(), endpoint.listenPort())
                .libraryAeronChannel(CommonContext.IPC_CHANNEL)
                .initialAcceptedSessionOwner(InitialAcceptedSessionOwner.SOLE_LIBRARY)
                .logFileDir(paths.logDir().toString())
                .deleteLogFileDirOnStart(false)
                .logInboundMessages(false)
                .logOutboundMessages(false)
                .defaultHeartbeatIntervalInS(30)
                .outboundLibraryFragmentLimit(config.performance().outboundFragmentLimit());

            configuration.aeronContext().aeronDirectoryName(paths.aeronDir().toString());
            configuration.aeronArchiveContext().aeronDirectoryName(paths.aeronDir().toString());
            return configuration;
        }

        private LibraryConfiguration buildLibraryConfiguration(EndpointPaths paths) {
            LibraryConfiguration configuration = new LibraryConfiguration()
                .libraryAeronChannels(List.of(CommonContext.IPC_CHANNEL))
                .sessionAcquireHandler(this::onSessionAcquired)
                .replyTimeoutInMs(config.shutdown().gracefulTimeoutMs())
                .libraryName("simulator-" + name)
                .defaultHeartbeatIntervalInS(30);

            configuration.aeronContext().aeronDirectoryName(paths.aeronDir().toString());
            return configuration;
        }

        private MediaDriver launchMediaDriver(Path aeronDir) {
            MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(aeronDir.toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
            return MediaDriver.launchEmbedded(context);
        }

        private SessionHandler onSessionAcquired(Session session, SessionAcquiredInfo info) {
            CompositeKey key = session.compositeKey();
            if (key != null && matchesEndpoint(key)) {
                sessionRef.set(session);
                acquired.set(true);
            } else {
                String message = "Acquired unexpected session on " + name + ": " + String.valueOf(key);
                lastError.compareAndSet(null, new IllegalStateException(message));
                LOGGER.warn(message);
            }
            return NoopSessionHandler.INSTANCE;
        }

        private boolean matchesEndpoint(CompositeKey key) {
            return endpoint.localCompId().equals(key.localCompId()) &&
                endpoint.remoteCompId().equals(key.remoteCompId());
        }

        private void startPolling(FixLibrary activeLibrary, int fragmentLimit) {
            running = true;
            Thread thread = new Thread(() -> {
                while (running) {
                    try {
                        int work = activeLibrary.poll(fragmentLimit);
                        if (work == 0) {
                            LockSupport.parkNanos(POLL_IDLE_NANOS);
                        }
                    } catch (RuntimeException pollFailure) {
                        lastError.compareAndSet(
                            null,
                            new IllegalStateException("Artio library poll loop failed for " + name, pollFailure)
                        );
                        running = false;
                    }
                }
            }, "artio-simulator-" + name + "-poller");
            thread.setDaemon(true);
            pollThread = thread;
            thread.start();
        }

        private void awaitLibraryConnected(FixLibrary activeLibrary) {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(STARTUP_TIMEOUT_MS);
            int fragmentLimit = config.performance().inboundFragmentLimit();
            while (!activeLibrary.isConnected()) {
                activeLibrary.poll(fragmentLimit);
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException("Timed out waiting for Artio library connection: " + name);
                }
                LockSupport.parkNanos(POLL_IDLE_NANOS);
            }
        }
    }

    private static void ensureDirectory(Path path) {
        try {
            Files.createDirectories(path);
        } catch (IOException failure) {
            throw new IllegalStateException("Unable to create directory: " + path, failure);
        }
    }

    private record EndpointPaths(Path workDir, Path aeronDir, Path logDir) {
    }

    public record Diagnostics(
        boolean entrySessionAcquired,
        boolean exitSessionAcquired,
        boolean entrySessionConnected,
        boolean exitSessionConnected,
        int sessionsAcquired,
        int queueDepth,
        String lastError
    ) {
    }

    private static final class NoopSessionHandler implements SessionHandler {
        private static final NoopSessionHandler INSTANCE = new NoopSessionHandler();

        @Override
        public Action onMessage(
            org.agrona.DirectBuffer buffer,
            int offset,
            int length,
            int libraryId,
            Session session,
            int sequenceIndex,
            long messageType,
            long timestampInNs,
            long position,
            OnMessageInfo messageInfo
        ) {
            return Action.CONTINUE;
        }

        @Override
        public void onTimeout(int libraryId, Session session) {
            // No-op.
        }

        @Override
        public void onSlowStatus(int libraryId, Session session, boolean hasBecomeSlow) {
            // No-op.
        }

        @Override
        public Action onDisconnect(int libraryId, Session session, DisconnectReason reason) {
            return Action.CONTINUE;
        }

        @Override
        public void onSessionStart(Session session) {
            // No-op.
        }
    }
}
