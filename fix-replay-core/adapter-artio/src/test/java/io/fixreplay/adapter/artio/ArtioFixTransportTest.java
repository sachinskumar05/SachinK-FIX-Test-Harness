package io.fixreplay.adapter.artio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.fixreplay.model.FixMessage;
import io.fixreplay.runner.SessionKey;
import io.fixreplay.runner.TransportSessionConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class ArtioFixTransportTest {
    @Test
    void wiresConnectSendReceiveAndCloseThroughGateway() {
        FakeArtioGateway outboundGateway = new FakeArtioGateway();
        FakeArtioGateway inboundGateway = new FakeArtioGateway();
        ArtioFixTransport transport = new ArtioFixTransport(gatewayFactory(outboundGateway, inboundGateway));
        List<FixMessage> received = new ArrayList<>();
        transport.onReceive(received::add);

        transport.connect(sessionConfig());
        assertNotNull(outboundGateway.connectedConfig);
        assertEquals("127.0.0.1", outboundGateway.connectedConfig.host());
        assertEquals(7001, outboundGateway.connectedConfig.port());
        assertEquals("ENTRY", outboundGateway.connectedConfig.senderCompId());
        assertEquals("VENUE", outboundGateway.connectedConfig.targetCompId());
        assertEquals("EXIT", outboundGateway.connectedConfig.exitSenderCompId());
        assertEquals("ENTRY", outboundGateway.connectedConfig.exitTargetCompId());

        assertNotNull(inboundGateway.connectedConfig);
        assertEquals("127.0.0.1", inboundGateway.connectedConfig.host());
        assertEquals(7002, inboundGateway.connectedConfig.port());
        assertEquals("ENTRY", inboundGateway.connectedConfig.senderCompId());
        assertEquals("EXIT", inboundGateway.connectedConfig.targetCompId());

        FixMessage outbound = FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=001|", '|');
        transport.send(outbound);
        assertEquals(1, outboundGateway.sentMessages.size());
        assertEquals("ORD-1", outboundGateway.sentMessages.get(0).getString(11));

        inboundGateway.emit(FixMessage.fromRaw("8=FIX.4.4|35=8|49=EXIT|56=ENTRY|37=EX-1|17=FILL-1|10=022|", '|'));
        inboundGateway.emit(FixMessage.fromRaw("8=FIX.4.4|35=8|49=OTHER|56=ENTRY|37=EX-2|17=FILL-2|10=023|", '|'));

        assertEquals(1, received.size());
        assertEquals("EX-1", received.get(0).getString(37));

        transport.close();
        assertTrue(outboundGateway.closed);
        assertTrue(inboundGateway.closed);
    }

    @Test
    void closesGatewayIfConnectFails() {
        FakeArtioGateway failingOutboundGateway = new FakeArtioGateway();
        failingOutboundGateway.connectFailure = new IllegalStateException("boom");
        FakeArtioGateway inboundGateway = new FakeArtioGateway();
        try (ArtioFixTransport transport = new ArtioFixTransport(gatewayFactory(failingOutboundGateway, inboundGateway))) {
            assertThrows(IllegalStateException.class, () -> transport.connect(sessionConfig()));
            assertTrue(failingOutboundGateway.closed);
            assertTrue(inboundGateway.closed);
        }
    }

    @Test
    void rejectsSendBeforeConnect() {
        try (
            ArtioFixTransport transport = new ArtioFixTransport(
                gatewayFactory(new FakeArtioGateway(), new FakeArtioGateway())
            )
        ) {
            assertThrows(
                    IllegalStateException.class,
                    () -> transport.send(FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-1|10=001|", '|')));
        }
    }

    private static TransportSessionConfig sessionConfig() {
        return new TransportSessionConfig(
                new SessionKey("ENTRY", "VENUE"),
                new SessionKey("EXIT", "ENTRY"),
                Map.of(
                        "artio.host",
                        "127.0.0.1",
                        "artio.port",
                        "7001",
                        "artio.exitHost",
                        "127.0.0.1",
                        "artio.exitPort",
                        "7002",
                        "artio.heartbeatSeconds",
                        "15",
                        "artio.replyTimeoutMs",
                        "4000"));
    }

    private static ArtioGatewayFactory gatewayFactory(FakeArtioGateway first, FakeArtioGateway second) {
        AtomicInteger index = new AtomicInteger();
        return () -> switch (index.getAndIncrement()) {
            case 0 -> first;
            case 1 -> second;
            default -> throw new IllegalStateException("Unexpected gateway request");
        };
    }

    private static final class FakeArtioGateway implements ArtioGateway {
        private Consumer<FixMessage> receiveHandler = ignored -> {
        };
        private final List<FixMessage> sentMessages = new ArrayList<>();
        private ArtioTransportConfig connectedConfig;
        private RuntimeException connectFailure;
        private boolean closed;

        @Override
        public void setReceiveHandler(Consumer<FixMessage> callback) {
            this.receiveHandler = callback;
        }

        @Override
        public void connect(ArtioTransportConfig config) {
            if (connectFailure != null) {
                throw connectFailure;
            }
            this.connectedConfig = config;
        }

        @Override
        public void send(FixMessage message) {
            sentMessages.add(message);
        }

        @Override
        public void close() {
            closed = true;
        }

        private void emit(FixMessage message) {
            receiveHandler.accept(message);
        }
    }
}
