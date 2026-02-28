package io.fixreplay.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class FixReplayServerTest {
    @Test
    void resolvesVersionFromSystemProperty() {
        System.setProperty("fix.replay.version", "1.2.3-test");
        try {
            FixReplayServer server = new FixReplayServer();
            assertEquals("1.2.3-test", server.currentVersion());
        } finally {
            System.clearProperty("fix.replay.version");
        }
    }
}
