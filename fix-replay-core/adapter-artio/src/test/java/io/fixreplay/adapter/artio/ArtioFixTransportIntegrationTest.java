package io.fixreplay.adapter.artio;

import io.fixreplay.model.FixMessage;
import io.fixreplay.runner.SessionKey;
import io.fixreplay.runner.TransportSessionConfig;
import java.util.Map;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("Manual test: requires a running Artio FixEngine and external FIX server")
class ArtioFixTransportIntegrationTest {
    @Test
    void manualSmokeTest() {
        TransportSessionConfig sessionConfig = new TransportSessionConfig(
            new SessionKey("CLIENT", "BROKER"),
            new SessionKey("BROKER", "CLIENT"),
            Map.of(
                "artio.host",
                "127.0.0.1",
                "artio.port",
                "9999",
                "artio.heartbeatSeconds",
                "30",
                "artio.connectTimeoutMs",
                "5000"
            )
        );

        try (ArtioFixTransport transport = new ArtioFixTransport()) {
            transport.onReceive(message -> {
                // Intentionally empty - this test is disabled by default.
            });
            transport.connect(sessionConfig);
            transport.send(FixMessage.fromRaw("8=FIX.4.4|35=0|49=CLIENT|56=BROKER|10=001|", '|'));
        }
    }
}

