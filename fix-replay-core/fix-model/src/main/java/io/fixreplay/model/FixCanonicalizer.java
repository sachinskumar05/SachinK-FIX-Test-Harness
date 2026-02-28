package io.fixreplay.model;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class FixCanonicalizer {
    public static final char SOH = '\u0001';

    private FixCanonicalizer() {
    }

    public static byte[] normalize(byte[] raw) {
        byte[] out = new byte[raw.length];
        int write = 0;
        for (int read = 0; read < raw.length; read++) {
            byte value = raw[read];
            if (value == 0x01 || value == '|') {
                out[write++] = 0x01;
                continue;
            }
            if (value == '^' && read + 1 < raw.length && raw[read + 1] == 'A') {
                out[write++] = 0x01;
                read++;
                continue;
            }
            out[write++] = value;
        }
        if (write == out.length) {
            return out;
        }
        byte[] compact = new byte[write];
        System.arraycopy(out, 0, compact, 0, write);
        return compact;
    }

    public static byte[] normalize(String raw) {
        return normalize(raw.getBytes(StandardCharsets.ISO_8859_1));
    }

    public static String normalizeToString(String raw) {
        return new String(normalize(raw), StandardCharsets.ISO_8859_1);
    }

    public static String toDisplayString(String raw, boolean orderByTag) {
        byte[] canonical = normalize(raw);
        if (!orderByTag) {
            return canonicalToDisplay(canonical);
        }
        List<FixField> fields = parseFields(canonical);
        fields.sort(Comparator.comparingInt(FixField::tag));
        return buildDisplay(fields);
    }

    private static String canonicalToDisplay(byte[] canonical) {
        StringBuilder out = new StringBuilder(canonical.length);
        for (byte value : canonical) {
            out.append(value == 0x01 ? '|' : (char) (value & 0xFF));
        }
        return out.toString();
    }

    private static List<FixField> parseFields(byte[] canonical) {
        List<FixField> fields = new ArrayList<>();
        int index = 0;
        while (index < canonical.length) {
            while (index < canonical.length && canonical[index] == 0x01) {
                index++;
            }
            if (index >= canonical.length) {
                break;
            }

            int tag = 0;
            int tagStart = index;
            while (index < canonical.length && canonical[index] >= '0' && canonical[index] <= '9') {
                tag = (tag * 10) + (canonical[index] - '0');
                index++;
            }
            if (index == tagStart || index >= canonical.length || canonical[index] != '=') {
                while (index < canonical.length && canonical[index] != 0x01) {
                    index++;
                }
                if (index < canonical.length && canonical[index] == 0x01) {
                    index++;
                }
                continue;
            }

            index++;
            int valueStart = index;
            while (index < canonical.length && canonical[index] != 0x01) {
                index++;
            }
            fields.add(new FixField(tag, new String(canonical, valueStart, index - valueStart, StandardCharsets.ISO_8859_1)));
            if (index < canonical.length && canonical[index] == 0x01) {
                index++;
            }
        }
        return fields;
    }

    private static String buildDisplay(List<FixField> fields) {
        StringBuilder out = new StringBuilder(fields.size() * 12);
        for (FixField field : fields) {
            out.append(field.tag()).append('=').append(field.value()).append('|');
        }
        return out.toString();
    }
}
