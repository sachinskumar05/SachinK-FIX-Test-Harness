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
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class ArtioFixTransportTest {
    @Test
    void wiresConnectSendReceiveAndCloseThroughGateway() {
        FakeArtioGateway gateway = new FakeArtioGateway();
        ArtioFixTransport transport = new ArtioFixTransport(() -> gateway);
        List<FixMessage> received = new ArrayList<>();
        transport.onReceive(received::add);

        transport.connect(sessionConfig());
        assertNotNull(gateway.connectedConfig);
        assertEquals("127.0.0.1", gateway.connectedConfig.host());
        assertEquals(7001, gateway.connectedConfig.port());
        assertEquals("ENTRY", gateway.connectedConfig.senderCompId());
        assertEquals("VENUE", gateway.connectedConfig.targetCompId());
        assertEquals("EXIT", gateway.connectedConfig.exitSenderCompId());
        assertEquals("ENTRY", gateway.connectedConfig.exitTargetCompId());

        FixMessage outbound = FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=001|", '|');
        transport.send(outbound);
        assertEquals(1, gateway.sentMessages.size());
        assertEquals("ORD-1", gateway.sentMessages.get(0).getString(11));

        gateway.emit(FixMessage.fromRaw("8=FIX.4.4|35=8|49=EXIT|56=ENTRY|37=EX-1|17=FILL-1|10=022|", '|'));
        gateway.emit(FixMessage.fromRaw("8=FIX.4.4|35=8|49=OTHER|56=ENTRY|37=EX-2|17=FILL-2|10=023|", '|'));

        assertEquals(1, received.size());
        assertEquals("EX-1", received.get(0).getString(37));

        transport.close();
        assertTrue(gateway.closed);
    }

    @Test
    void closesGatewayIfConnectFails() {
        FakeArtioGateway gateway = new FakeArtioGateway();
        gateway.connectFailure = new IllegalStateException("boom");
        ArtioFixTransport transport = new ArtioFixTransport(() -> gateway);

        assertThrows(IllegalStateException.class, () -> transport.connect(sessionConfig()));
        assertTrue(gateway.closed);
    }

    @Test
    void rejectsSendBeforeConnect() {
        ArtioFixTransport transport = new ArtioFixTransport(() -> new FakeArtioGateway());

        assertThrows(
            IllegalStateException.class,
            () -> transport.send(FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-1|10=001|", '|'))
        );
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
                "artio.heartbeatSeconds",
                "15",
                "artio.replyTimeoutMs",
                "4000"
            )
        );
    }

    private static final class FakeArtioGateway implements ArtioGateway {
        private Consumer<FixMessage> receiveHandler = ignored -> {};
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

