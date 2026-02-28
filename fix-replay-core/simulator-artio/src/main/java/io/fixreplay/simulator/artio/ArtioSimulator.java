package io.fixreplay.simulator.artio;

import io.aeron.CommonContext;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.fixreplay.model.FixCanonicalizer;
import io.fixreplay.model.FixMessage;
import io.fixreplay.model.FixParser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.AcquiringSessionExistsHandler;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.OnMessageInfo;
import uk.co.real_logic.artio.library.SessionAcquireHandler;
import uk.co.real_logic.artio.library.SessionAcquiredInfo;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner;
import uk.co.real_logic.artio.session.CompositeKey;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.validation.SessionPersistenceStrategy;

public final class ArtioSimulator implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArtioSimulator.class);
    private static final long STARTUP_TIMEOUT_MS = 15_000L;
    private static final long POLL_IDLE_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final Set<String> ADMIN_MSG_TYPES = Set.of("0", "1", "2", "4", "5", "A");

    private final ArtioSimulatorConfig config;
    private final ArtioMutationEngine mutationEngine;
    private final TopologyMode topologyMode;
    private final TopologyRuntime topologyRuntime;
    private final ArrayDeque<QueuedOutboundMessage> pendingOutboundQueue;
    private final Object routingLock = new Object();
    private final AtomicInteger queueDepth = new AtomicInteger();
    private final AtomicReference<Throwable> lastError = new AtomicReference<>();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    private ArtioSimulator(ArtioSimulatorConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.mutationEngine = ArtioMutationEngine.fromConfig(config.mutation());
        this.pendingOutboundQueue = new ArrayDeque<>(Math.min(1_024, config.routing().maxQueueDepth()));
        this.topologyMode = (config.entry().listenPort() == config.exit().listenPort())
            ? TopologyMode.SINGLE_PORT
            : TopologyMode.DUAL_PORT;
        this.topologyRuntime = topologyMode == TopologyMode.SINGLE_PORT
            ? new SinglePortRuntime(config.entry(), config.exit())
            : new DualPortRuntime(config.entry(), config.exit());
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
        return topologyRuntime.isReady();
    }

    public Diagnostics diagnosticsSnapshot() {
        Throwable error = lastError.get();
        int sessionsAcquired = 0;
        if (topologyRuntime.entrySessionAcquired()) {
            sessionsAcquired++;
        }
        if (topologyRuntime.exitSessionAcquired()) {
            sessionsAcquired++;
        }

        return new Diagnostics(
            topologyRuntime.entrySessionAcquired(),
            topologyRuntime.exitSessionAcquired(),
            topologyRuntime.entrySessionConnected(),
            topologyRuntime.exitSessionConnected(),
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
            topologyRuntime.stop();
        } catch (RuntimeException failure) {
            shutdownFailure = failure;
        }
        clearQueuedMessages();

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
        if (topologyMode == TopologyMode.SINGLE_PORT &&
            !config.entry().listenHost().equals(config.exit().listenHost())) {
            throw new IllegalStateException(
                "Single-port topology requires matching entry/exit listen_host values"
            );
        }

        try {
            topologyRuntime.start();
            LOGGER.info(
                "Artio simulator started in {} mode [entryPort={}, exitPort={}]",
                topologyMode.name().toLowerCase(),
                config.entry().listenPort(),
                config.exit().listenPort()
            );
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

    private interface TopologyRuntime {
        void start();

        void stop();

        boolean entrySessionAcquired();

        boolean exitSessionAcquired();

        boolean entrySessionConnected();

        boolean exitSessionConnected();

        default boolean isReady() {
            return entrySessionAcquired() && exitSessionAcquired() && entrySessionConnected() && exitSessionConnected();
        }
    }

    private final class DualPortRuntime implements TopologyRuntime {
        private final EndpointRuntime entryRuntime;
        private final EndpointRuntime exitRuntime;

        private DualPortRuntime(
            ArtioSimulatorConfig.SessionEndpoint entryEndpoint,
            ArtioSimulatorConfig.SessionEndpoint exitEndpoint
        ) {
            this.exitRuntime = new EndpointRuntime("exit", exitEndpoint, SessionRole.EXIT, () -> exitRuntimeSession());
            this.entryRuntime = new EndpointRuntime("entry", entryEndpoint, SessionRole.ENTRY, () -> exitRuntimeSession());
        }

        @Override
        public void start() {
            entryRuntime.start(endpointPath("entry"));
            exitRuntime.start(endpointPath("exit"));
        }

        @Override
        public void stop() {
            RuntimeException failure = null;
            try {
                exitRuntime.stop();
            } catch (RuntimeException stopFailure) {
                failure = stopFailure;
            }
            try {
                entryRuntime.stop();
            } catch (RuntimeException stopFailure) {
                if (failure == null) {
                    failure = stopFailure;
                } else {
                    failure.addSuppressed(stopFailure);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        public boolean entrySessionAcquired() {
            return entryRuntime.sessionAcquired();
        }

        @Override
        public boolean exitSessionAcquired() {
            return exitRuntime.sessionAcquired();
        }

        @Override
        public boolean entrySessionConnected() {
            return entryRuntime.sessionConnected();
        }

        @Override
        public boolean exitSessionConnected() {
            return exitRuntime.sessionConnected();
        }

        private Session exitRuntimeSession() {
            return exitRuntime.currentSession();
        }
    }

    private final class SinglePortRuntime extends EngineRuntime implements TopologyRuntime {
        private final ArtioSimulatorConfig.SessionEndpoint entryEndpoint;
        private final ArtioSimulatorConfig.SessionEndpoint exitEndpoint;

        private final AtomicReference<Session> entrySessionRef = new AtomicReference<>();
        private final AtomicReference<Session> exitSessionRef = new AtomicReference<>();
        private final AtomicBoolean entryAcquired = new AtomicBoolean();
        private final AtomicBoolean exitAcquired = new AtomicBoolean();

        private SinglePortRuntime(
            ArtioSimulatorConfig.SessionEndpoint entryEndpoint,
            ArtioSimulatorConfig.SessionEndpoint exitEndpoint
        ) {
            super("single");
            this.entryEndpoint = entryEndpoint;
            this.exitEndpoint = exitEndpoint;
        }

        @Override
        public void start() {
            startEngine(
                endpointPath("single"),
                entryEndpoint.listenHost(),
                entryEndpoint.listenPort(),
                this::onSessionAcquired
            );
        }

        @Override
        public void stop() {
            stopEngine();
            entrySessionRef.set(null);
            exitSessionRef.set(null);
            entryAcquired.set(false);
            exitAcquired.set(false);
        }

        @Override
        public boolean entrySessionAcquired() {
            return entryAcquired.get();
        }

        @Override
        public boolean exitSessionAcquired() {
            return exitAcquired.get();
        }

        @Override
        public boolean entrySessionConnected() {
            Session session = entrySessionRef.get();
            return session != null && session.isConnected();
        }

        @Override
        public boolean exitSessionConnected() {
            Session session = exitSessionRef.get();
            return session != null && session.isConnected();
        }

        private SessionHandler onSessionAcquired(Session session, SessionAcquiredInfo info) {
            CompositeKey key = session.compositeKey();
            if (key != null && matchesEndpoint(key, entryEndpoint)) {
                entrySessionRef.set(session);
                entryAcquired.set(true);
                return new RoutingSessionHandler(runtimeName(), SessionRole.ENTRY, () -> exitSessionRef.get());
            } else if (key != null && matchesEndpoint(key, exitEndpoint)) {
                exitSessionRef.set(session);
                exitAcquired.set(true);
                return new RoutingSessionHandler(runtimeName(), SessionRole.EXIT, () -> exitSessionRef.get());
            } else {
                LOGGER.warn("Ignoring unexpected session on single-port runtime: {}", key);
                return new RoutingSessionHandler(runtimeName(), SessionRole.OBSERVE_ONLY, () -> exitSessionRef.get());
            }
        }
    }

    private final class EndpointRuntime extends EngineRuntime {
        private final ArtioSimulatorConfig.SessionEndpoint endpoint;
        private final SessionRole role;
        private final Supplier<Session> exitSessionSupplier;
        private final AtomicReference<Session> sessionRef = new AtomicReference<>();
        private final AtomicBoolean acquired = new AtomicBoolean();

        private EndpointRuntime(
            String name,
            ArtioSimulatorConfig.SessionEndpoint endpoint,
            SessionRole role,
            Supplier<Session> exitSessionSupplier
        ) {
            super(name);
            this.endpoint = endpoint;
            this.role = role;
            this.exitSessionSupplier = exitSessionSupplier;
        }

        private void start(EndpointPaths paths) {
            startEngine(paths, endpoint.listenHost(), endpoint.listenPort(), this::onSessionAcquired);
        }

        private void stop() {
            stopEngine();
            sessionRef.set(null);
            acquired.set(false);
        }

        private boolean sessionAcquired() {
            return acquired.get();
        }

        private boolean sessionConnected() {
            Session session = sessionRef.get();
            return session != null && session.isConnected();
        }

        private Session currentSession() {
            return sessionRef.get();
        }

        private SessionHandler onSessionAcquired(Session session, SessionAcquiredInfo info) {
            CompositeKey key = session.compositeKey();
            if (key != null && matchesEndpoint(key, endpoint)) {
                sessionRef.set(session);
                acquired.set(true);
                return new RoutingSessionHandler(runtimeName(), role, exitSessionSupplier);
            } else {
                LOGGER.warn("Ignoring unexpected session on {} runtime: {}", runtimeName(), key);
                return new RoutingSessionHandler(runtimeName(), SessionRole.OBSERVE_ONLY, exitSessionSupplier);
            }
        }
    }

    private abstract class EngineRuntime {
        private final String runtimeName;
        private FixEngine engine;
        private FixLibrary library;
        private MediaDriver mediaDriver;
        private Thread pollThread;
        private volatile boolean running;

        private EngineRuntime(String runtimeName) {
            this.runtimeName = runtimeName;
        }

        protected String runtimeName() {
            return runtimeName;
        }

        protected void startEngine(
            EndpointPaths paths,
            String listenHost,
            int listenPort,
            SessionAcquireHandler sessionAcquireHandler
        ) {
            ensureDirectory(paths.workDir());
            ensureDirectory(paths.aeronDir());
            ensureDirectory(paths.logDir());

            EngineConfiguration engineConfiguration = buildEngineConfiguration(paths, listenHost, listenPort);
            LibraryConfiguration libraryConfiguration = buildLibraryConfiguration(paths, sessionAcquireHandler);

            MediaDriver launchedDriver = launchMediaDriver(paths.aeronDir());
            FixEngine launchedEngine = null;
            FixLibrary connectedLibrary = null;
            try {
                launchedEngine = FixEngine.launch(engineConfiguration);
                connectedLibrary = FixLibrary.connect(libraryConfiguration);
                awaitLibraryConnected(connectedLibrary);
                startPolling(connectedLibrary, config.performance().inboundFragmentLimit());
            } catch (RuntimeException failure) {
                closeQuietly(connectedLibrary, failure);
                closeQuietly(launchedEngine, failure);
                closeQuietly(launchedDriver, failure);
                throw failure;
            }

            this.mediaDriver = launchedDriver;
            this.engine = launchedEngine;
            this.library = connectedLibrary;
            LOGGER.info(
                "Artio runtime '{}' listening on {}:{}",
                runtimeName,
                listenHost,
                listenPort
            );
        }

        protected void stopEngine() {
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

            if (failure != null) {
                throw failure;
            }
        }

        private EngineConfiguration buildEngineConfiguration(
            EndpointPaths paths,
            String listenHost,
            int listenPort
        ) {
            EngineConfiguration configuration = new EngineConfiguration()
                .bindTo(listenHost, listenPort)
                .libraryAeronChannel(CommonContext.IPC_CHANNEL)
                .initialAcceptedSessionOwner(InitialAcceptedSessionOwner.SOLE_LIBRARY)
                .logFileDir(paths.logDir().toString())
                .deleteLogFileDirOnStart(true)
                .logInboundMessages(false)
                .logOutboundMessages(false)
                .sessionPersistenceStrategy(SessionPersistenceStrategy.alwaysTransient())
                .defaultHeartbeatIntervalInS(30)
                .outboundLibraryFragmentLimit(config.performance().outboundFragmentLimit());

            configuration.aeronContext().aeronDirectoryName(paths.aeronDir().toString());
            configuration.aeronArchiveContext().aeronDirectoryName(paths.aeronDir().toString());
            return configuration;
        }

        private LibraryConfiguration buildLibraryConfiguration(
            EndpointPaths paths,
            SessionAcquireHandler sessionAcquireHandler
        ) {
            LibraryConfiguration configuration = new LibraryConfiguration()
                .libraryAeronChannels(List.of(CommonContext.IPC_CHANNEL))
                .sessionExistsHandler(new AcquiringSessionExistsHandler(false))
                .sessionAcquireHandler(sessionAcquireHandler)
                .replyTimeoutInMs(config.shutdown().gracefulTimeoutMs())
                .libraryName("simulator-" + runtimeName)
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
                            new IllegalStateException("Artio library poll loop failed for " + runtimeName, pollFailure)
                        );
                        running = false;
                    }
                }
            }, "artio-simulator-" + runtimeName + "-poller");
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
                    throw new IllegalStateException("Timed out waiting for Artio library connection: " + runtimeName);
                }
                LockSupport.parkNanos(POLL_IDLE_NANOS);
            }
        }

        private <T extends AutoCloseable> void closeQuietly(T closeable, RuntimeException failure) {
            if (closeable == null) {
                return;
            }
            try {
                closeable.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    private boolean matchesEndpoint(CompositeKey key, ArtioSimulatorConfig.SessionEndpoint endpoint) {
        return endpoint.localCompId().equals(key.localCompId()) &&
            endpoint.remoteCompId().equals(key.remoteCompId());
    }

    private Action handleEntryMessage(
        String runtimeName,
        DirectBuffer buffer,
        int offset,
        int length,
        Session entrySession,
        Supplier<Session> exitSessionSupplier
    ) {
        try {
            byte[] raw = new byte[length];
            buffer.getBytes(offset, raw, 0, length);
            byte[] canonical = FixCanonicalizer.normalize(raw);
            FixMessage inbound = FixParser.parse(canonical);

            String msgType = inbound.msgType();
            if (msgType == null || msgType.isBlank()) {
                LOGGER.warn("Ignoring inbound ENTRY message without tag 35 [runtime={}, sessionId={}]", runtimeName, entrySession.id());
                return Action.CONTINUE;
            }
            if (!shouldRouteMessageType(msgType)) {
                return Action.CONTINUE;
            }

            Map<Integer, String> outboundFields = new HashMap<>(inbound.fields());
            if (config.mutation().enabled()) {
                mutationEngine.apply(msgType, outboundFields);
            }

            String outboundMsgType = outboundFields.get(35);
            if (outboundMsgType == null || outboundMsgType.isBlank()) {
                throw new IllegalStateException("Mutated message is missing required tag 35");
            }

            if (config.observability().logInboundOutbound()) {
                LOGGER.debug(
                    "Routing ENTRY message [runtime={}, sessionId={}, msgType={}, length={}]",
                    runtimeName,
                    entrySession.id(),
                    outboundMsgType,
                    length
                );
            }

            return routeToExit(runtimeName, outboundMsgType, outboundFields, exitSessionSupplier.get());
        } catch (RuntimeException processingFailure) {
            lastError.compareAndSet(
                null,
                new IllegalStateException("ENTRY routing failure on runtime " + runtimeName, processingFailure)
            );
            LOGGER.warn("Failed to process inbound ENTRY message on runtime {}", runtimeName, processingFailure);
            return Action.CONTINUE;
        }
    }

    private boolean shouldRouteMessageType(String msgType) {
        if (config.routing().dropAdminMessages() && ADMIN_MSG_TYPES.contains(msgType)) {
            return false;
        }
        return config.routing().enabledMsgTypes().contains(msgType);
    }

    private Action routeToExit(String runtimeName, String msgType, Map<Integer, String> fields, Session exitSession) {
        if (exitSession == null || !exitSession.isConnected()) {
            return handleExitUnavailable(msgType, fields);
        }

        Action flushResult = flushQueuedMessages(runtimeName, exitSession);
        if (flushResult != Action.CONTINUE) {
            return flushResult;
        }

        return sendToExit(runtimeName, exitSession, new QueuedOutboundMessage(msgType, fields));
    }

    private Action handleExitUnavailable(String msgType, Map<Integer, String> fields) {
        if (config.routing().failIfExitNotLoggedOn()) {
            IllegalStateException failure = new IllegalStateException(
                "EXIT session not connected while routing message type " + msgType
            );
            lastError.compareAndSet(null, failure);
            LOGGER.warn("Dropping message because EXIT session is not connected [msgType={}]", msgType);
            return Action.CONTINUE;
        }

        synchronized (routingLock) {
            if (pendingOutboundQueue.size() >= config.routing().maxQueueDepth()) {
                IllegalStateException failure = new IllegalStateException(
                    "EXIT pending queue depth exceeded " + config.routing().maxQueueDepth()
                );
                lastError.compareAndSet(null, failure);
                LOGGER.warn("EXIT pending queue is full; applying backpressure");
                return Action.ABORT;
            }
            pendingOutboundQueue.addLast(new QueuedOutboundMessage(msgType, new HashMap<>(fields)));
            queueDepth.incrementAndGet();
        }
        return Action.CONTINUE;
    }

    private Action flushQueuedMessages(String runtimeName, Session exitSession) {
        synchronized (routingLock) {
            while (!pendingOutboundQueue.isEmpty()) {
                QueuedOutboundMessage queued = pendingOutboundQueue.peekFirst();
                if (queued == null) {
                    return Action.CONTINUE;
                }
                Action sendResult = sendToExit(runtimeName, exitSession, queued);
                if (sendResult != Action.CONTINUE) {
                    return sendResult;
                }
                pendingOutboundQueue.removeFirst();
                queueDepth.decrementAndGet();
            }
        }
        return Action.CONTINUE;
    }

    private Action sendToExit(
        String runtimeName,
        Session exitSession,
        QueuedOutboundMessage message
    ) {
        if (exitSession == null || !exitSession.isConnected()) {
            return Action.CONTINUE;
        }

        long delayMs = config.routing().artificialDelayMs();
        if (delayMs > 0) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(delayMs));
        }

        int sequenceNumber = Math.max(1, exitSession.lastSentMsgSeqNum() + 1);
        ArtioOutboundCodec.EncodedFixMessage encoded = ArtioOutboundCodec.encode(
            message.fields(),
            config.beginString(),
            config.exit().localCompId(),
            config.exit().remoteCompId(),
            sequenceNumber
        );

        UnsafeBuffer outboundBuffer = new UnsafeBuffer(encoded.payload());
        long sendResult = exitSession.trySend(
            outboundBuffer,
            0,
            encoded.payload().length,
            encoded.seqNum(),
            encoded.packedMsgType()
        );

        if (sendResult <= 0) {
            LOGGER.warn(
                "EXIT trySend backpressured/failed [runtime={}, msgType={}, code={}]",
                runtimeName,
                encoded.msgType(),
                sendResult
            );
            return Action.ABORT;
        }

        if (config.observability().logInboundOutbound()) {
            LOGGER.debug(
                "Sent EXIT message [runtime={}, sessionId={}, msgType={}, seqNum={}]",
                runtimeName,
                exitSession.id(),
                encoded.msgType(),
                encoded.seqNum()
            );
        }
        if (config.observability().logFixPayloads()) {
            LOGGER.debug("EXIT payload {}", formatPayload(encoded.payload()));
        }
        return Action.CONTINUE;
    }

    private void clearQueuedMessages() {
        synchronized (routingLock) {
            pendingOutboundQueue.clear();
            queueDepth.set(0);
        }
    }

    private static String formatPayload(byte[] payload) {
        return new String(payload, StandardCharsets.ISO_8859_1).replace('\u0001', '|');
    }

    private static void ensureDirectory(Path path) {
        try {
            Files.createDirectories(path);
        } catch (IOException failure) {
            throw new IllegalStateException("Unable to create directory: " + path, failure);
        }
    }

    private enum TopologyMode {
        SINGLE_PORT,
        DUAL_PORT
    }

    private enum SessionRole {
        ENTRY,
        EXIT,
        OBSERVE_ONLY
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

    private record QueuedOutboundMessage(String msgType, Map<Integer, String> fields) {
        private QueuedOutboundMessage {
            Objects.requireNonNull(msgType, "msgType");
            fields = Map.copyOf(Objects.requireNonNull(fields, "fields"));
        }
    }

    private final class RoutingSessionHandler implements SessionHandler {
        private final String runtimeName;
        private final SessionRole sessionRole;
        private final Supplier<Session> exitSessionSupplier;

        private RoutingSessionHandler(
            String runtimeName,
            SessionRole sessionRole,
            Supplier<Session> exitSessionSupplier
        ) {
            this.runtimeName = runtimeName;
            this.sessionRole = sessionRole;
            this.exitSessionSupplier = exitSessionSupplier;
        }

        @Override
        public Action onMessage(
            DirectBuffer buffer,
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
            if (sessionRole == SessionRole.ENTRY) {
                return handleEntryMessage(runtimeName, buffer, offset, length, session, exitSessionSupplier);
            }

            if (sessionRole == SessionRole.EXIT) {
                Action flushResult = flushQueuedMessages(runtimeName, session);
                if (flushResult != Action.CONTINUE) {
                    return flushResult;
                }
            }

            if (config.observability().logInboundOutbound() && sessionRole != SessionRole.OBSERVE_ONLY) {
                LOGGER.debug(
                    "Observed message [runtime={}, role={}, sessionId={}, seqIndex={}, messageType={}, length={}]",
                    runtimeName,
                    sessionRole,
                    session.id(),
                    sequenceIndex,
                    messageType,
                    length
                );
            }
            if (config.observability().logFixPayloads() && sessionRole != SessionRole.OBSERVE_ONLY) {
                byte[] payload = new byte[length];
                buffer.getBytes(offset, payload, 0, length);
                LOGGER.debug("Observed payload [runtime={}, role={}] {}", runtimeName, sessionRole, formatPayload(payload));
            }
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
            if (config.observability().logInboundOutbound() && sessionRole != SessionRole.OBSERVE_ONLY) {
                LOGGER.debug(
                    "Session disconnected [runtime={}, role={}, sessionId={}, reason={}]",
                    runtimeName,
                    sessionRole,
                    session.id(),
                    reason
                );
            }
            return Action.CONTINUE;
        }

        @Override
        public void onSessionStart(Session session) {
            if (sessionRole == SessionRole.EXIT) {
                flushQueuedMessages(runtimeName, session);
            }
        }
    }
}
