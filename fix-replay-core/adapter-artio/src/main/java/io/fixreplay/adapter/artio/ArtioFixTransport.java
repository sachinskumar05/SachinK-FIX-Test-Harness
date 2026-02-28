package io.fixreplay.adapter.artio;

import io.fixreplay.model.FixMessage;
import io.fixreplay.runner.FixTransport;
import io.fixreplay.runner.TransportSessionConfig;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public final class ArtioFixTransport implements FixTransport {
    private static final Consumer<FixMessage> NOOP_RECEIVER = ignored -> {};

    private final ArtioGatewayFactory gatewayFactory;
    private final Object lifecycleLock = new Object();

    private volatile Consumer<FixMessage> receiveCallback = NOOP_RECEIVER;
    private ArtioGateway outboundGateway;
    private ArtioGateway inboundGateway;
    private ArtioTransportConfig outboundConfig;
    private String expectedInboundSenderCompId;
    private String expectedInboundTargetCompId;

    public ArtioFixTransport() {
        this(RealArtioGateway::new);
    }

    ArtioFixTransport(ArtioGatewayFactory gatewayFactory) {
        this.gatewayFactory = Objects.requireNonNull(gatewayFactory, "gatewayFactory");
    }

    @Override
    public void connect(TransportSessionConfig sessionConfig) {
        Objects.requireNonNull(sessionConfig, "sessionConfig");

        ArtioGateway createdOutboundGateway;
        ArtioGateway createdInboundGateway;
        ArtioTransportConfig createdOutboundConfig = ArtioTransportConfig.from(sessionConfig);
        ArtioTransportConfig createdInboundConfig = inboundConfig(sessionConfig.properties(), createdOutboundConfig);
        synchronized (lifecycleLock) {
            if (outboundGateway != null || inboundGateway != null) {
                throw new IllegalStateException("Artio transport is already connected");
            }

            createdOutboundGateway = gatewayFactory.create();
            createdInboundGateway = gatewayFactory.create();
            createdOutboundGateway.setReceiveHandler(NOOP_RECEIVER);
            createdInboundGateway.setReceiveHandler(this::forwardInboundMessage);

            outboundConfig = createdOutboundConfig;
            expectedInboundSenderCompId = createdOutboundConfig.exitSenderCompId();
            expectedInboundTargetCompId = createdOutboundConfig.exitTargetCompId();
            outboundGateway = createdOutboundGateway;
            inboundGateway = createdInboundGateway;
        }

        try {
            createdOutboundGateway.connect(createdOutboundConfig);
            createdInboundGateway.connect(createdInboundConfig);
        } catch (RuntimeException connectFailure) {
            onConnectFailure(createdOutboundGateway, createdInboundGateway, connectFailure);
            throw connectFailure;
        }
    }

    @Override
    public void onReceive(Consumer<FixMessage> callback) {
        receiveCallback = Objects.requireNonNull(callback, "callback");
        ArtioGateway activeGateway;
        synchronized (lifecycleLock) {
            activeGateway = inboundGateway;
        }
        if (activeGateway != null) {
            activeGateway.setReceiveHandler(this::forwardInboundMessage);
        }
    }

    @Override
    public void send(FixMessage message) {
        ArtioGateway activeGateway;
        synchronized (lifecycleLock) {
            activeGateway = outboundGateway;
        }
        if (activeGateway == null) {
            throw new IllegalStateException("Artio transport is not connected");
        }
        activeGateway.send(message);
    }

    @Override
    public void close() {
        ArtioGateway outboundToClose;
        ArtioGateway inboundToClose;
        synchronized (lifecycleLock) {
            outboundToClose = outboundGateway;
            inboundToClose = inboundGateway;
            outboundGateway = null;
            inboundGateway = null;
            outboundConfig = null;
            expectedInboundSenderCompId = null;
            expectedInboundTargetCompId = null;
        }
        RuntimeException closeFailure = null;
        if (inboundToClose != null) {
            try {
                inboundToClose.close();
            } catch (RuntimeException failure) {
                closeFailure = failure;
            }
        }
        if (outboundToClose != null) {
            try {
                outboundToClose.close();
            } catch (RuntimeException failure) {
                if (closeFailure == null) {
                    closeFailure = failure;
                } else {
                    closeFailure.addSuppressed(failure);
                }
            }
        }
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    private void onConnectFailure(
        ArtioGateway failedOutboundGateway,
        ArtioGateway failedInboundGateway,
        RuntimeException connectFailure
    ) {
        synchronized (lifecycleLock) {
            if (outboundGateway == failedOutboundGateway) {
                outboundGateway = null;
            }
            if (inboundGateway == failedInboundGateway) {
                inboundGateway = null;
            }
            outboundConfig = null;
            expectedInboundSenderCompId = null;
            expectedInboundTargetCompId = null;
        }
        closeGatewayQuietly(failedInboundGateway, connectFailure);
        closeGatewayQuietly(failedOutboundGateway, connectFailure);
    }

    private static void closeGatewayQuietly(ArtioGateway gateway, RuntimeException connectFailure) {
        if (gateway == null) {
            return;
        }
        try {
            gateway.close();
        } catch (RuntimeException closeFailure) {
            connectFailure.addSuppressed(closeFailure);
        }
    }

    private void forwardInboundMessage(FixMessage message) {
        String expectedSender;
        String expectedTarget;
        synchronized (lifecycleLock) {
            expectedSender = expectedInboundSenderCompId;
            expectedTarget = expectedInboundTargetCompId;
        }
        if (expectedSender == null || expectedTarget == null || shouldDispatch(message, expectedSender, expectedTarget)) {
            receiveCallback.accept(message);
        }
    }

    private static ArtioTransportConfig inboundConfig(Map<String, String> properties, ArtioTransportConfig outboundConfig) {
        String host = readString(properties, ArtioTransportConfig.EXIT_HOST_PROPERTY, outboundConfig.host());
        int port = readInt(properties, ArtioTransportConfig.EXIT_PORT_PROPERTY, outboundConfig.port());

        String senderCompId = readString(
            properties,
            ArtioTransportConfig.EXIT_TARGET_COMP_ID_PROPERTY,
            outboundConfig.exitTargetCompId()
        );
        String targetCompId = readString(
            properties,
            ArtioTransportConfig.EXIT_SENDER_COMP_ID_PROPERTY,
            outboundConfig.exitSenderCompId()
        );
        String aeronDir = readString(properties, ArtioTransportConfig.EXIT_AERON_DIR_PROPERTY, outboundConfig.aeronDir());

        return new ArtioTransportConfig(
            host,
            port,
            outboundConfig.beginString(),
            senderCompId,
            targetCompId,
            outboundConfig.exitSenderCompId(),
            outboundConfig.exitTargetCompId(),
            outboundConfig.username(),
            outboundConfig.password(),
            outboundConfig.heartbeatIntervalSeconds(),
            outboundConfig.replyTimeoutMs(),
            outboundConfig.connectTimeoutMs(),
            outboundConfig.pollFragmentLimit(),
            outboundConfig.pollIdleMicros(),
            outboundConfig.sequenceNumbersPersistent(),
            outboundConfig.resetSeqNum(),
            outboundConfig.disconnectOnFirstMessageNotLogon(),
            outboundConfig.aeronChannel(),
            aeronDir
        );
    }

    private static String readString(Map<String, String> properties, String key, String defaultValue) {
        String value = properties.get(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value.trim();
    }

    private static int readInt(Map<String, String> properties, String key, int defaultValue) {
        String value = properties.get(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException parseFailure) {
            throw new IllegalArgumentException("Invalid integer property " + key + ": " + value, parseFailure);
        }
    }

    private static boolean shouldDispatch(FixMessage message, String expectedSenderCompId, String expectedTargetCompId) {
        String senderCompId = message.senderCompId();
        String targetCompId = message.targetCompId();
        if (senderCompId == null || targetCompId == null) {
            return true;
        }
        return expectedSenderCompId.equals(senderCompId) && expectedTargetCompId.equals(targetCompId);
    }
}

@FunctionalInterface
interface ArtioGatewayFactory {
    ArtioGateway create();
}
