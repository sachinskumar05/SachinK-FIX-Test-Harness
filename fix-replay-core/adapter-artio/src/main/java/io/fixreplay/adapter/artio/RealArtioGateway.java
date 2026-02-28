package io.fixreplay.adapter.artio;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.fixreplay.model.FixMessage;
import io.fixreplay.model.FixParser;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.OnMessageInfo;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.session.Session;

final class RealArtioGateway implements ArtioGateway {
    private static final Consumer<FixMessage> NOOP_RECEIVER = ignored -> {};
    private static final long CLOSE_JOIN_TIMEOUT_MS = 1_000L;

    private final Object stateLock = new Object();
    private final AtomicReference<Consumer<FixMessage>> receiveHandler = new AtomicReference<>(NOOP_RECEIVER);
    private final AtomicReference<RuntimeException> pollFailure = new AtomicReference<>();

    private FixLibrary library;
    private Session session;
    private ArtioTransportConfig config;
    private Thread pollerThread;
    private boolean running;
    private int nextSequenceNumber = 1;

    @Override
    public void setReceiveHandler(Consumer<FixMessage> callback) {
        receiveHandler.set(Objects.requireNonNull(callback, "callback"));
    }

    @Override
    public void connect(ArtioTransportConfig transportConfig) {
        Objects.requireNonNull(transportConfig, "transportConfig");
        synchronized (stateLock) {
            if (library != null) {
                throw new IllegalStateException("Artio gateway is already connected");
            }

            this.config = transportConfig;
            this.pollFailure.set(null);

            FixLibrary connectedLibrary = null;
            try {
                LibraryConfiguration libraryConfiguration = new LibraryConfiguration();
                libraryConfiguration.sessionAcquireHandler(
                    (acquiredSession, acquiredInfo) -> {
                        session = acquiredSession;
                        return new InboundSessionHandler(receiveHandler);
                    }
                );
                libraryConfiguration.replyTimeoutInMs(transportConfig.replyTimeoutMs());
                libraryConfiguration.defaultHeartbeatIntervalInS(transportConfig.heartbeatIntervalSeconds());
                if (transportConfig.aeronChannel() != null) {
                    libraryConfiguration.libraryAeronChannels(List.of(transportConfig.aeronChannel()));
                }

                connectedLibrary = FixLibrary.connect(libraryConfiguration);
                library = connectedLibrary;
                awaitLibraryConnection();

                SessionConfiguration sessionConfiguration = buildSessionConfiguration(transportConfig);
                Reply<Session> initiateReply = connectedLibrary.initiate(sessionConfiguration);
                session = awaitReply(initiateReply, "session initiate");
                nextSequenceNumber = Math.max(1, session.lastSentMsgSeqNum() + 1);

                startPoller();
            } catch (RuntimeException connectFailure) {
                cleanupFailedConnect(connectedLibrary, connectFailure);
                throw connectFailure;
            }
        }
    }

    @Override
    public void send(FixMessage message) {
        Objects.requireNonNull(message, "message");
        synchronized (stateLock) {
            ensureConnected();
            throwIfPollFailed();

            ArtioWireMessageCodec.EncodedFixMessage encoded = ArtioWireMessageCodec.encode(message, config, nextSequenceNumber);
            UnsafeBuffer buffer = new UnsafeBuffer(encoded.payload());
            long result = session.trySend(buffer, 0, encoded.payload().length, encoded.seqNum(), encoded.messageType());
            if (result <= 0) {
                throw new IllegalStateException("Artio trySend failed with code " + result);
            }
            nextSequenceNumber = Math.max(nextSequenceNumber + 1, encoded.seqNum() + 1);
        }
    }

    @Override
    public void close() {
        Thread threadToJoin;
        FixLibrary libraryToClose;
        synchronized (stateLock) {
            running = false;
            threadToJoin = pollerThread;
            pollerThread = null;
            libraryToClose = library;
            library = null;
            session = null;
            config = null;
            nextSequenceNumber = 1;
            pollFailure.set(null);
        }

        if (threadToJoin != null && threadToJoin != Thread.currentThread()) {
            threadToJoin.interrupt();
            try {
                threadToJoin.join(CLOSE_JOIN_TIMEOUT_MS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        if (libraryToClose != null) {
            libraryToClose.close();
        }
    }

    private void startPoller() {
        running = true;
        Thread thread = new Thread(this::pollLoop, "artio-fix-transport-poller");
        thread.setDaemon(true);
        pollerThread = thread;
        thread.start();
    }

    private void pollLoop() {
        while (running) {
            try {
                FixLibrary activeLibrary;
                ArtioTransportConfig activeConfig;
                synchronized (stateLock) {
                    activeLibrary = library;
                    activeConfig = config;
                }
                if (activeLibrary == null || activeConfig == null) {
                    return;
                }

                int work = activeLibrary.poll(activeConfig.pollFragmentLimit());
                if (work == 0 && activeConfig.pollIdleMicros() > 0) {
                    LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(activeConfig.pollIdleMicros()));
                }
            } catch (RuntimeException pollException) {
                pollFailure.compareAndSet(null, new IllegalStateException("Artio poll loop failed", pollException));
                running = false;
            }
        }
    }

    private void ensureConnected() {
        if (library == null || session == null || config == null) {
            throw new IllegalStateException("Artio gateway is not connected");
        }
    }

    private void throwIfPollFailed() {
        RuntimeException failure = pollFailure.get();
        if (failure != null) {
            throw failure;
        }
    }

    private void awaitLibraryConnection() {
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.connectTimeoutMs());
        while (!library.isConnected()) {
            library.poll(config.pollFragmentLimit());
            if (System.nanoTime() >= deadlineNanos) {
                throw new IllegalStateException("Timed out waiting for Artio library connection");
            }
            if (config.pollIdleMicros() > 0) {
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(config.pollIdleMicros()));
            }
        }
    }

    private <T> T awaitReply(Reply<T> reply, String operationName) {
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.connectTimeoutMs());
        while (reply.isExecuting()) {
            library.poll(config.pollFragmentLimit());
            if (System.nanoTime() >= deadlineNanos) {
                throw new IllegalStateException("Timed out waiting for Artio " + operationName);
            }
            if (config.pollIdleMicros() > 0) {
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(config.pollIdleMicros()));
            }
        }

        if (reply.hasCompleted()) {
            T result = reply.resultIfPresent();
            if (result == null) {
                throw new IllegalStateException("Artio " + operationName + " completed without a result");
            }
            return result;
        }
        if (reply.hasErrored()) {
            throw new IllegalStateException("Artio " + operationName + " failed", reply.error());
        }
        if (reply.hasTimedOut()) {
            throw new IllegalStateException("Artio " + operationName + " timed out");
        }
        throw new IllegalStateException("Artio " + operationName + " finished in unexpected state " + reply.state());
    }

    private SessionConfiguration buildSessionConfiguration(ArtioTransportConfig transportConfig) {
        SessionConfiguration.Builder builder = SessionConfiguration.builder()
            .address(transportConfig.host(), transportConfig.port())
            .senderCompId(transportConfig.senderCompId())
            .targetCompId(transportConfig.targetCompId())
            .sequenceNumbersPersistent(transportConfig.sequenceNumbersPersistent())
            .resetSeqNum(transportConfig.resetSeqNum())
            .disconnectOnFirstMessageNotLogon(transportConfig.disconnectOnFirstMessageNotLogon())
            .timeoutInMs(transportConfig.replyTimeoutMs());

        if (transportConfig.username() != null || transportConfig.password() != null) {
            builder.credentials(defaultString(transportConfig.username()), defaultString(transportConfig.password()));
        }

        return builder.build();
    }

    private String defaultString(String value) {
        return value == null ? "" : value;
    }

    private void cleanupFailedConnect(FixLibrary connectedLibrary, RuntimeException connectFailure) {
        running = false;
        Thread thread = pollerThread;
        pollerThread = null;
        if (thread != null && thread != Thread.currentThread()) {
            thread.interrupt();
            try {
                thread.join(CLOSE_JOIN_TIMEOUT_MS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        session = null;
        library = null;
        config = null;
        nextSequenceNumber = 1;
        pollFailure.set(null);

        if (connectedLibrary != null) {
            try {
                connectedLibrary.close();
            } catch (RuntimeException closeFailure) {
                connectFailure.addSuppressed(closeFailure);
            }
        }
    }

    private static final class InboundSessionHandler implements SessionHandler {
        private final AtomicReference<Consumer<FixMessage>> receiver;

        private InboundSessionHandler(AtomicReference<Consumer<FixMessage>> receiver) {
            this.receiver = receiver;
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
            receiver.get().accept(FixParser.parse(raw));
            return Action.CONTINUE;
        }

        @Override
        public void onTimeout(int libraryId, Session session) {
            // No-op for adapter wiring.
        }

        @Override
        public void onSlowStatus(int libraryId, Session session, boolean hasBecomeSlow) {
            // No-op for adapter wiring.
        }

        @Override
        public Action onDisconnect(int libraryId, Session session, DisconnectReason reason) {
            return Action.CONTINUE;
        }

        @Override
        public void onSessionStart(Session session) {
            // No-op for adapter wiring.
        }
    }
}

