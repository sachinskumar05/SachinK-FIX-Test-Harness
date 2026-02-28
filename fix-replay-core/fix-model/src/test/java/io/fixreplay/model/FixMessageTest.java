package io.fixreplay.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class FixMessageTest {
    private static final char SOH = '\u0001';

    @Test
    void parsesCanonicalMessageWithRequiredTags() {
        String canonical = "8=FIX.4.4" + SOH + "9=112" + SOH + "35=D" + SOH + "49=BUY1" + SOH + "56=SELL1" + SOH + "10=128" + SOH;
        FixMessage message = FixParser.parse(canonical);

        assertEquals("FIX.4.4", message.getString(8));
        assertEquals(112, message.getInt(9));
        assertEquals("D", message.msgType());
        assertEquals("BUY1", message.senderCompId());
        assertEquals("SELL1", message.targetCompId());
        assertEquals("128", message.getString(10));
    }

    @Test
    void parsesFromByteArrayAndPreservesLowercaseMsgType() {
        String canonical = "8=FIX.4.4" + SOH + "9=99" + SOH + "35=j" + SOH + "10=003" + SOH;
        FixMessage message = FixParser.parse(canonical.getBytes(StandardCharsets.ISO_8859_1));

        assertEquals("j", message.msgType());
        assertEquals("003", message.getString(10));
    }

    @Test
    void keepsLastTagValueAndTracksDuplicates() {
        String canonical = "8=FIX.4.4" + SOH + "35=D" + SOH + "35=G" + SOH + "35=F" + SOH + "10=100" + SOH;
        FixMessage message = FixParser.parse(canonical);

        assertEquals("F", message.msgType());
        assertTrue(message.hasDuplicates());
        assertEquals(List.of("D", "G", "F"), message.duplicateValues(35).orElseThrow());
    }

    @Test
    void normalizesDelimiterVariantsToCanonicalSoh() {
        String raw = "8=FIX.4.4|9=77^A35=D" + SOH + "10=001|";

        String canonical = FixCanonicalizer.normalizeToString(raw);
        FixMessage message = FixParser.parse(canonical);

        assertEquals("FIX.4.4", message.getString(8));
        assertEquals(77, message.getInt(9));
        assertEquals("D", message.msgType());
        assertEquals("001", message.getString(10));
    }

    @Test
    void appliesDefaultMsgTypeFilterSet() {
        MsgTypeFilter filter = new MsgTypeFilter();

        FixMessage allowedLowercase = FixParser.parse("35=j" + SOH + "10=001" + SOH);
        FixMessage blocked = FixParser.parse("35=Z" + SOH + "10=001" + SOH);

        assertTrue(filter.accepts(allowedLowercase));
        assertFalse(filter.accepts(blocked));
    }

    @Test
    void buildsOrderedDisplayWhenRequested() {
        String raw = "35=D|8=FIX.4.2|10=999|";

        String ordered = FixCanonicalizer.toDisplayString(raw, true);

        assertEquals("8=FIX.4.2|10=999|35=D|", ordered);
    }
}
