package io.fixreplay.simulator.artio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.fixreplay.model.FixMessage;
import io.fixreplay.model.FixParser;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ArtioSessionMessageSenderTest {
    @Test
    void encodesDeterministicPayloadWithPreparedHeaderValues() {
        Map<Integer, String> fields = new HashMap<>();
        fields.put(49, "SHOULD_NOT_BE_USED");
        fields.put(56, "SHOULD_NOT_BE_USED");
        fields.put(34, "999");
        fields.put(52, "19990101-00:00:00.000");
        fields.put(35, "D");
        fields.put(55, "IBM");
        fields.put(11, "CL-123");
        fields.put(60, "20260228-10:00:00.000");

        FixMessage message = ArtioSessionMessageSender.toFixMessage(fields);
        ArtioSessionMessageSender.PreparedHeaderState header = new ArtioSessionMessageSender.PreparedHeaderState(
            42,
            "FIX_GATEWAY",
            "EXIT_RACOMPID",
            "20260228-12:00:00.000",
            "D"
        );

        byte[] encoded = ArtioSessionMessageSender.encodePayload(message, "FIX.4.2", header, 4096);
        FixMessage decoded = FixParser.parse(encoded);

        assertEquals("FIX.4.2", decoded.getString(8));
        assertEquals("D", decoded.getString(35));
        assertEquals("FIX_GATEWAY", decoded.getString(49));
        assertEquals("EXIT_RACOMPID", decoded.getString(56));
        assertEquals("42", decoded.getString(34));
        assertEquals("20260228-12:00:00.000", decoded.getString(52));
        assertEquals("CL-123", decoded.getString(11));
        assertEquals("IBM", decoded.getString(55));
        assertEquals("20260228-10:00:00.000", decoded.getString(60));
        assertTrue(decoded.getString(9) != null && !decoded.getString(9).isBlank());
        assertTrue(decoded.getString(10) != null && !decoded.getString(10).isBlank());

        String display = ArtioSessionMessageSender.toDisplayString(encoded);
        assertTrue(display.indexOf("|35=") < display.indexOf("|49="));
        assertTrue(display.indexOf("|49=") < display.indexOf("|56="));
        assertTrue(display.indexOf("|56=") < display.indexOf("|34="));
        assertTrue(display.indexOf("|34=") < display.indexOf("|52="));
        assertTrue(display.indexOf("|11=") < display.indexOf("|55="));
        assertTrue(display.indexOf("|55=") < display.indexOf("|60="));
    }

    @Test
    void toFixMessageRequiresMsgType() {
        Map<Integer, String> fields = new HashMap<>();
        fields.put(11, "CL-001");

        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> ArtioSessionMessageSender.toFixMessage(fields)
        );
        assertTrue(error.getMessage().contains("tag 35"));
    }
}
