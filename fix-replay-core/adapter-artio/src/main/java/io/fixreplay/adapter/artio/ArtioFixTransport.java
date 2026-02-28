package io.fixreplay.adapter.artio;

import io.fixreplay.model.FixMessage;
import io.fixreplay.runner.FixTransport;
import io.fixreplay.runner.TransportSessionConfig;
import java.util.Objects;
import java.util.function.Consumer;

public final class ArtioFixTransport implements FixTransport {
    private static final Consumer<FixMessage> NOOP_RECEIVER = ignored -> {};

    private final ArtioGatewayFactory gatewayFactory;
    private final Object lifecycleLock = new Object();

    private volatile Consumer<FixMessage> receiveCallback = NOOP_RECEIVER;
    private ArtioGateway gateway;
    private ArtioTransportConfig activeConfig;

    public ArtioFixTransport() {
        this(RealArtioGateway::new);
    }

    ArtioFixTransport(ArtioGatewayFactory gatewayFactory) {
        this.gatewayFactory = Objects.requireNonNull(gatewayFactory, "gatewayFactory");
    }

    @Override
    public void connect(TransportSessionConfig sessionConfig) {
        Objects.requireNonNull(sessionConfig, "sessionConfig");

        ArtioGateway connectedGateway;
        synchronized (lifecycleLock) {
            if (gateway != null) {
                throw new IllegalStateException("Artio transport is already connected");
            }

            ArtioTransportConfig config = ArtioTransportConfig.from(sessionConfig);
            activeConfig = config;
            connectedGateway = gatewayFactory.create();
            connectedGateway.setReceiveHandler(this::forwardInboundMessage);
            gateway = connectedGateway;
        }

        try {
            connectedGateway.connect(activeConfig);
        } catch (RuntimeException connectFailure) {
            onConnectFailure(connectedGateway, connectFailure);
            throw connectFailure;
        }
    }

    @Override
    public void onReceive(Consumer<FixMessage> callback) {
        receiveCallback = Objects.requireNonNull(callback, "callback");
        ArtioGateway activeGateway;
        synchronized (lifecycleLock) {
            activeGateway = gateway;
        }
        if (activeGateway != null) {
            activeGateway.setReceiveHandler(this::forwardInboundMessage);
        }
    }

    @Override
    public void send(FixMessage message) {
        ArtioGateway activeGateway;
        synchronized (lifecycleLock) {
            activeGateway = gateway;
        }
        if (activeGateway == null) {
            throw new IllegalStateException("Artio transport is not connected");
        }
        activeGateway.send(message);
    }

    @Override
    public void close() {
        ArtioGateway gatewayToClose;
        synchronized (lifecycleLock) {
            gatewayToClose = gateway;
            gateway = null;
            activeConfig = null;
        }
        if (gatewayToClose != null) {
            gatewayToClose.close();
        }
    }

    private void onConnectFailure(ArtioGateway failedGateway, RuntimeException connectFailure) {
        synchronized (lifecycleLock) {
            if (gateway == failedGateway) {
                gateway = null;
                activeConfig = null;
            }
        }
        try {
            failedGateway.close();
        } catch (RuntimeException closeFailure) {
            connectFailure.addSuppressed(closeFailure);
        }
    }

    private void forwardInboundMessage(FixMessage message) {
        ArtioTransportConfig config = activeConfig;
        if (config == null || shouldDispatch(message, config)) {
            receiveCallback.accept(message);
        }
    }

    private static boolean shouldDispatch(FixMessage message, ArtioTransportConfig config) {
        String senderCompId = message.senderCompId();
        String targetCompId = message.targetCompId();
        if (senderCompId == null || targetCompId == null) {
            return true;
        }
        return config.exitSenderCompId().equals(senderCompId) && config.exitTargetCompId().equals(targetCompId);
    }
}

@FunctionalInterface
interface ArtioGatewayFactory {
    ArtioGateway create();
}

