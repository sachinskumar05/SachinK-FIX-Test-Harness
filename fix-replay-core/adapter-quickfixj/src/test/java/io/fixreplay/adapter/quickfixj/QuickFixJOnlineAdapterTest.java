package io.fixreplay.adapter.quickfixj;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fixreplay.model.FixMessage;
import org.junit.jupiter.api.Test;

class QuickFixJOnlineAdapterTest {
    @Test
    void recordsOutboundMessages() {
        QuickFixJOnlineAdapter adapter = new QuickFixJOnlineAdapter();
        adapter.send(FixMessage.fromRaw("8=FIX.4.2|35=D|11=ABC123|", '|'));

        assertEquals("quickfixj", adapter.name());
        assertEquals(1, adapter.sentCount());
    }
}
