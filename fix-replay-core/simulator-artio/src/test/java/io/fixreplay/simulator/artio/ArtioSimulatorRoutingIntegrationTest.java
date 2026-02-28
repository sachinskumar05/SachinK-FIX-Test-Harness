package io.fixreplay.simulator.artio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.aeron.CommonContext;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.fixreplay.model.FixMessage;
import io.fixreplay.model.FixParser;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.OnMessageInfo;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.validation.SessionPersistenceStrategy;

class ArtioSimulatorRoutingIntegrationTest {
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(20);
    private static final Duration RECEIVE_TIMEOUT = Duration.ofSeconds(10);

    private ArtioSimulator simulator;
    private TestArtioInitiator entryInitiator;
    private TestArtioInitiator exitInitiator;

    @AfterEach
    void tearDown() {
        closeQuietly(entryInitiator);
        closeQuietly(exitInitiator);
        closeQuietly(simulator);
    }

    @Test
    void routesEntryToExitAndAppliesMutationRules(@TempDir Path tempDir) throws Exception {
        int entryPort = freePort();
        int exitPort = freePort();
        Path scenario = tempDir.resolve("scenario.yaml");
        Files.writeString(
            scenario,
            """
                simulator:
                  provider: artio
                  enabled: true
                  entry:
                    listen_host: 127.0.0.1
                    listen_port: %d
                  exit:
                    listen_host: 127.0.0.1
                    listen_port: %d
                  routing:
                    enabled_msg_types: [D, G, F, 8, 3, j]
                    drop_admin_messages: true
                    artificial_delay_ms: 0
                    fail_if_exit_not_logged_on: true
                    max_queue_depth: 1000
                mutation:
                  enabled: true
                  strict_mode: false
                  rules_inline:
                    rules:
                      - name: RA_prefix_ClOrdID
                        when:
                          msgTypes: [D, G, F]
                          conditions:
                            - tag: 11
                              exists: true
                        actions:
                          - type: prefix
                            tag: 11
                            value: RA-
                      - name: RA_set_custom_tag
                        when:
                          msgTypes: [D]
                        actions:
                          - type: set
                            tag: 9001
                            value: RAPID_ADDITION
                """.formatted(entryPort, exitPort)
        );

        ArtioSimulatorConfig config = ArtioSimulatorConfig.load(scenario);
        simulator = ArtioSimulator.start(config);

        entryInitiator = TestArtioInitiator.start(
            tempDir.resolve("entry-initiator"),
            "127.0.0.1",
            entryPort,
            "ENTRY_RACOMPID",
            "FIX_GATEWAY"
        );
        exitInitiator = TestArtioInitiator.start(
            tempDir.resolve("exit-initiator"),
            "127.0.0.1",
            exitPort,
            "EXIT_RACOMPID",
            "FIX_GATEWAY"
        );

        awaitTrue(() -> simulator.isReady(), STARTUP_TIMEOUT, "simulator ready");
        awaitTrue(() -> entryInitiator.isConnected(), STARTUP_TIMEOUT, "entry initiator connected");
        awaitTrue(() -> exitInitiator.isConnected(), STARTUP_TIMEOUT, "exit initiator connected");

        FixMessage inboundOrder = FixParser.parse(
            "35=D\u000111=ORDER-001\u000155=IBM\u000154=1\u000160=20260228-12:00:00.000\u0001"
        );
        entryInitiator.send(inboundOrder);

        FixMessage routed = exitInitiator.awaitMessage(
            message -> "D".equals(message.msgType()) && "RA-ORDER-001".equals(message.getString(11)),
            RECEIVE_TIMEOUT
        );
        assertNotNull(routed);
        assertEquals("D", routed.msgType());
        assertEquals("RA-ORDER-001", routed.getString(11));
        assertEquals("RAPID_ADDITION", routed.getString(9001));
        assertEquals("FIX_GATEWAY", routed.getString(49));
        assertEquals("EXIT_RACOMPID", routed.getString(56));
    }

    private static void awaitTrue(CheckedBooleanSupplier condition, Duration timeout, String description) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        fail("Timed out waiting for " + description);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // ignore test cleanup failure
        }
    }

    @FunctionalInterface
    private interface CheckedBooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }

    private static final class TestArtioInitiator implements AutoCloseable {
        private static final long TIMEOUT_MS = 10_000L;
        private static final int FRAGMENT_LIMIT = 50;
        private static final long IDLE_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

        private final Path workDir;
        private final Path aeronDir;
        private final Path logDir;
        private final String host;
        private final int port;
        private final String senderCompId;
        private final String targetCompId;
        private final LinkedBlockingQueue<FixMessage> inbound = new LinkedBlockingQueue<>();
        private final ArtioSessionMessageSender sender = new ArtioSessionMessageSender();
        private final AtomicReference<Throwable> pollFailure = new AtomicReference<>();
        private final AtomicBoolean running = new AtomicBoolean();

        private MediaDriver mediaDriver;
        private FixEngine engine;
        private FixLibrary library;
        private Session session;
        private Thread pollThread;

        private TestArtioInitiator(Path rootDir, String host, int port, String senderCompId, String targetCompId) {
            this.workDir = rootDir.toAbsolutePath().normalize();
            this.aeronDir = workDir.resolve("aeron");
            this.logDir = workDir.resolve("logs");
            this.host = host;
            this.port = port;
            this.senderCompId = senderCompId;
            this.targetCompId = targetCompId;
        }

        private static TestArtioInitiator start(
            Path rootDir,
            String host,
            int port,
            String senderCompId,
            String targetCompId
        ) {
            TestArtioInitiator initiator = new TestArtioInitiator(rootDir, host, port, senderCompId, targetCompId);
            initiator.start();
            return initiator;
        }

        private void start() {
            ensureDirectory(workDir);
            ensureDirectory(aeronDir);
            ensureDirectory(logDir);

            MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
                .aeronDirectoryName(aeronDir.toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
            mediaDriver = MediaDriver.launchEmbedded(mediaDriverContext);

            EngineConfiguration engineConfiguration = new EngineConfiguration()
                .libraryAeronChannel(CommonContext.IPC_CHANNEL)
                .logFileDir(logDir.toString())
                .deleteLogFileDirOnStart(true)
                .logInboundMessages(false)
                .logOutboundMessages(false)
                .sessionPersistenceStrategy(SessionPersistenceStrategy.alwaysTransient())
                .defaultHeartbeatIntervalInS(30);
            engineConfiguration.aeronContext().aeronDirectoryName(aeronDir.toString());
            engineConfiguration.aeronArchiveContext().aeronDirectoryName(aeronDir.toString());
            engine = FixEngine.launch(engineConfiguration);

            LibraryConfiguration libraryConfiguration = new LibraryConfiguration()
                .libraryAeronChannels(List.of(CommonContext.IPC_CHANNEL))
                .sessionAcquireHandler((acquiredSession, info) -> {
                    session = acquiredSession;
                    return new TestSessionHandler(inbound);
                })
                .replyTimeoutInMs(TIMEOUT_MS)
                .libraryName("test-initiator-" + senderCompId + "-" + port)
                .defaultHeartbeatIntervalInS(30);
            libraryConfiguration.aeronContext().aeronDirectoryName(aeronDir.toString());
            library = FixLibrary.connect(libraryConfiguration);

            awaitLibraryConnected();

            SessionConfiguration configuration = SessionConfiguration.builder()
                .address(host, port)
                .senderCompId(senderCompId)
                .targetCompId(targetCompId)
                .sequenceNumbersPersistent(false)
                .resetSeqNum(true)
                .disconnectOnFirstMessageNotLogon(true)
                .timeoutInMs(TIMEOUT_MS)
                .build();
            session = awaitReply(library.initiate(configuration), "initiator connect");
            startPolling();
        }

        private void startPolling() {
            running.set(true);
            pollThread = new Thread(() -> {
                while (running.get()) {
                    try {
                        int work = library.poll(FRAGMENT_LIMIT);
                        if (work == 0) {
                            java.util.concurrent.locks.LockSupport.parkNanos(IDLE_NANOS);
                        }
                    } catch (RuntimeException failure) {
                        pollFailure.compareAndSet(null, failure);
                        running.set(false);
                    }
                }
            }, "artio-test-initiator-poller-" + senderCompId + "-" + port);
            pollThread.setDaemon(true);
            pollThread.start();
        }

        private void awaitLibraryConnected() {
            long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
            while (!library.isConnected()) {
                library.poll(FRAGMENT_LIMIT);
                if (System.nanoTime() >= deadlineNanos) {
                    throw new IllegalStateException("Timed out waiting for initiator library connection");
                }
            }
        }

        private Session awaitReply(Reply<Session> reply, String operationName) {
            long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
            while (reply.isExecuting()) {
                library.poll(FRAGMENT_LIMIT);
                if (System.nanoTime() >= deadlineNanos) {
                    throw new IllegalStateException("Timed out during " + operationName);
                }
            }
            if (reply.hasCompleted()) {
                Session result = reply.resultIfPresent();
                if (result == null) {
                    throw new IllegalStateException("Reply completed without session: " + operationName);
                }
                return result;
            }
            if (reply.hasErrored()) {
                throw new IllegalStateException("Reply errored during " + operationName, reply.error());
            }
            if (reply.hasTimedOut()) {
                throw new IllegalStateException("Reply timed out during " + operationName);
            }
            throw new IllegalStateException("Unexpected reply state during " + operationName + ": " + reply.state());
        }

        private boolean isConnected() {
            Throwable failure = pollFailure.get();
            if (failure != null) {
                throw new IllegalStateException("Initiator poll loop failed", failure);
            }
            return session != null && session.isConnected();
        }

        private void send(FixMessage message) {
            Objects.requireNonNull(message, "message");
            if (!isConnected()) {
                throw new IllegalStateException("Initiator is not connected");
            }
            long result = sender.sendOnSession(session, message, "FIX.4.2");
            if (result <= 0) {
                throw new IllegalStateException("Failed to send message via initiator, code=" + result);
            }
        }

        private FixMessage awaitMessage(Predicate<FixMessage> predicate, Duration timeout) throws InterruptedException {
            Instant deadline = Instant.now().plus(timeout);
            while (Instant.now().isBefore(deadline)) {
                FixMessage message = inbound.poll(25, TimeUnit.MILLISECONDS);
                if (message != null && predicate.test(message)) {
                    return message;
                }
            }
            return null;
        }

        @Override
        public void close() {
            running.set(false);
            Thread thread = pollThread;
            pollThread = null;
            if (thread != null && thread != Thread.currentThread()) {
                thread.interrupt();
                try {
                    thread.join(TIMEOUT_MS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            if (library != null) {
                library.close();
                library = null;
            }
            if (engine != null) {
                engine.close();
                engine = null;
            }
            if (mediaDriver != null) {
                mediaDriver.close();
                mediaDriver = null;
            }
        }

        private static void ensureDirectory(Path path) {
            try {
                Files.createDirectories(path);
            } catch (IOException failure) {
                throw new IllegalStateException("Failed to create test directory: " + path, failure);
            }
        }
    }

    private static final class TestSessionHandler implements SessionHandler {
        private final LinkedBlockingQueue<FixMessage> inbound;

        private TestSessionHandler(LinkedBlockingQueue<FixMessage> inbound) {
            this.inbound = inbound;
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
            byte[] raw = new byte[length];
            buffer.getBytes(offset, raw, 0, length);
            inbound.offer(FixParser.parse(raw));
            return Action.CONTINUE;
        }

        @Override
        public void onTimeout(int libraryId, Session session) {
            // no-op
        }

        @Override
        public void onSlowStatus(int libraryId, Session session, boolean hasBecomeSlow) {
            // no-op
        }

        @Override
        public Action onDisconnect(int libraryId, Session session, DisconnectReason reason) {
            return Action.CONTINUE;
        }

        @Override
        public void onSessionStart(Session session) {
            // no-op
        }
    }
}
