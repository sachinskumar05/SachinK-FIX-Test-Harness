package io.fixreplay.simulator.artio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.fixreplay.model.FixParser;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ArtioOutboundCodecTest {
    @Test
    void encodesCanonicalFixPayloadForExitSession() {
        Map<Integer, String> fields = new HashMap<>();
        fields.put(35, "D");
        fields.put(11, "CLORD-1");
        fields.put(55, "IBM");
        fields.put(54, "1");

        ArtioOutboundCodec.EncodedFixMessage encoded = ArtioOutboundCodec.encode(
            fields,
            "FIX.4.2",
            "FIX_GATEWAY",
            "EXIT_RACOMPID",
            7
        );

        assertEquals("D", encoded.msgType());
        assertEquals(7, encoded.seqNum());

        var parsed = FixParser.parse(encoded.payload());
        assertEquals("FIX.4.2", parsed.getString(8));
        assertEquals("D", parsed.getString(35));
        assertEquals("FIX_GATEWAY", parsed.getString(49));
        assertEquals("EXIT_RACOMPID", parsed.getString(56));
        assertEquals("7", parsed.getString(34));
        assertEquals("CLORD-1", parsed.getString(11));
        assertNotNull(parsed.getString(9));
        assertNotNull(parsed.getString(10));
    }
}
