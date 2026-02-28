package io.fixreplay.model;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class FixParser {
    private static final byte SOH = 0x01;

    private FixParser() {
    }

    public static FixMessage parse(String canonicalRaw) {
        return parse(canonicalRaw.getBytes(StandardCharsets.ISO_8859_1));
    }

    public static FixMessage parse(byte[] canonicalRaw) {
        int expectedFields = Math.max(4, canonicalRaw.length / 8);
        FixMessage.IntStringTable tags = new FixMessage.IntStringTable(expectedFields);
        Map<Integer, List<String>> duplicates = null;

        int index = 0;
        while (index < canonicalRaw.length) {
            while (index < canonicalRaw.length && isIgnoredLeadingByte(canonicalRaw[index])) {
                index++;
            }
            if (index >= canonicalRaw.length) {
                break;
            }

            int tag = 0;
            int tagStart = index;
            while (index < canonicalRaw.length && isDigit(canonicalRaw[index])) {
                tag = (tag * 10) + (canonicalRaw[index] - '0');
                index++;
            }

            if (index == tagStart || index >= canonicalRaw.length || canonicalRaw[index] != '=') {
                index = skipToDelimiter(canonicalRaw, index);
                continue;
            }
            index++;

            int valueStart = index;
            while (index < canonicalRaw.length && canonicalRaw[index] != SOH) {
                index++;
            }

            if (tag > 0) {
                String value = new String(canonicalRaw, valueStart, index - valueStart, StandardCharsets.ISO_8859_1);
                String previous = tags.put(tag, value);
                if (previous != null) {
                    duplicates = addDuplicate(duplicates, tag, previous, value);
                }
            }

            if (index < canonicalRaw.length && canonicalRaw[index] == SOH) {
                index++;
            }
        }

        return new FixMessage(tags, duplicates);
    }

    static FixMessage parse(String raw, char delimiter) {
        if (delimiter == '\u0001') {
            return parse(raw);
        }
        byte[] canonical = FixCanonicalizer.normalize(raw);
        byte delimiterByte = (byte) delimiter;
        for (int i = 0; i < canonical.length; i++) {
            if (canonical[i] == delimiterByte) {
                canonical[i] = SOH;
            }
        }
        return parse(canonical);
    }

    private static Map<Integer, List<String>> addDuplicate(
        Map<Integer, List<String>> duplicates,
        int tag,
        String previous,
        String current
    ) {
        Map<Integer, List<String>> duplicateMap = duplicates;
        if (duplicateMap == null) {
            duplicateMap = new HashMap<>();
        }
        List<String> values = duplicateMap.get(tag);
        if (values == null) {
            values = new ArrayList<>(4);
            values.add(previous);
            duplicateMap.put(tag, values);
        }
        values.add(current);
        return duplicateMap;
    }

    private static boolean isDigit(byte value) {
        return value >= '0' && value <= '9';
    }

    private static boolean isIgnoredLeadingByte(byte value) {
        return value == SOH || value == '\n' || value == '\r';
    }

    private static int skipToDelimiter(byte[] canonicalRaw, int index) {
        int cursor = index;
        while (cursor < canonicalRaw.length && canonicalRaw[cursor] != SOH) {
            cursor++;
        }
        if (cursor < canonicalRaw.length && canonicalRaw[cursor] == SOH) {
            cursor++;
        }
        return cursor;
    }
}
