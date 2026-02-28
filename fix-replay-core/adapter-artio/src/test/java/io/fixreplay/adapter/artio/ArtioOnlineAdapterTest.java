package io.fixreplay.adapter.artio;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fixreplay.model.FixMessage;
import org.junit.jupiter.api.Test;

class ArtioOnlineAdapterTest {
    @Test
    void recordsOutboundMessages() {
        ArtioOnlineAdapter adapter = new ArtioOnlineAdapter();
        adapter.send(FixMessage.fromRaw("8=FIX.4.2|35=D|11=ABC123|", '|'));

        assertEquals("artio", adapter.name());
        assertEquals(1, adapter.sentCount());
    }
}
