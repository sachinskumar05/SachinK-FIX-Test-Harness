package io.fixreplay.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class FixMessageTest {
    @Test
    void parsesMessageTokens() {
        FixMessage message = FixMessage.fromRaw("8=FIX.4.2|35=D|11=ABC123|", '|');

        assertEquals("FIX.4.2", message.get(8));
        assertEquals("D", message.get(35));
        assertEquals("ABC123", message.get(11));
    }
}
