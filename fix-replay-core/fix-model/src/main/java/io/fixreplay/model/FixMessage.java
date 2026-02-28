package io.fixreplay.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

public final class FixMessage {
    private final IntStringTable fields;
    private final Map<Integer, List<String>> duplicates;
    private Map<Integer, String> boxedFields;

    FixMessage(IntStringTable fields, Map<Integer, List<String>> duplicates) {
        this.fields = fields;
        this.duplicates = freezeDuplicates(duplicates);
    }

    public static FixMessage fromRaw(String raw, char delimiter) {
        return FixParser.parse(raw, delimiter);
    }

    public static FixMessage fromRaw(String raw) {
        return FixParser.parse(raw);
    }

    public String get(int tag) {
        return fields.get(tag);
    }

    public String getString(int tag) {
        return fields.get(tag);
    }

    public int getInt(int tag) {
        String value = fields.get(tag);
        if (value == null) {
            throw new NoSuchElementException("Tag " + tag + " is not present");
        }
        return Integer.parseInt(value);
    }

    public int getInt(int tag, int defaultValue) {
        String value = fields.get(tag);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    public String msgType() {
        return fields.get(35);
    }

    public String senderCompId() {
        return fields.get(49);
    }

    public String targetCompId() {
        return fields.get(56);
    }

    public Optional<List<String>> duplicateValues(int tag) {
        List<String> values = duplicates.get(tag);
        return values == null ? Optional.empty() : Optional.of(values);
    }

    public boolean hasDuplicates() {
        return !duplicates.isEmpty();
    }

    public Map<Integer, String> fields() {
        if (boxedFields == null) {
            boxedFields = Collections.unmodifiableMap(fields.toMap());
        }
        return boxedFields;
    }

    private static Map<Integer, List<String>> freezeDuplicates(Map<Integer, List<String>> source) {
        if (source == null || source.isEmpty()) {
            return Map.of();
        }
        Map<Integer, List<String>> out = new HashMap<>(source.size());
        for (Map.Entry<Integer, List<String>> entry : source.entrySet()) {
            out.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return Map.copyOf(out);
    }

    static final class IntStringTable {
        private static final float MAX_LOAD = 0.6f;

        private int[] keys;
        private String[] values;
        private int size;
        private int threshold;

        IntStringTable(int expectedSize) {
            int capacity = 8;
            int required = Math.max(4, expectedSize);
            while (capacity < (required * 2)) {
                capacity <<= 1;
            }
            this.keys = new int[capacity];
            this.values = new String[capacity];
            this.threshold = (int) (capacity * MAX_LOAD);
        }

        String put(int key, String value) {
            if (key <= 0) {
                throw new IllegalArgumentException("FIX tag must be positive: " + key);
            }
            if (size >= threshold) {
                resize();
            }
            int slot = findSlot(key, keys);
            if (keys[slot] == 0) {
                keys[slot] = key;
                values[slot] = value;
                size++;
                return null;
            }
            String previous = values[slot];
            values[slot] = value;
            return previous;
        }

        String get(int key) {
            if (key <= 0) {
                return null;
            }
            int mask = keys.length - 1;
            int slot = mix(key) & mask;
            while (true) {
                int current = keys[slot];
                if (current == 0) {
                    return null;
                }
                if (current == key) {
                    return values[slot];
                }
                slot = (slot + 1) & mask;
            }
        }

        Map<Integer, String> toMap() {
            Map<Integer, String> out = new HashMap<>(Math.max(4, size * 2));
            for (int i = 0; i < keys.length; i++) {
                int key = keys[i];
                if (key != 0) {
                    out.put(key, values[i]);
                }
            }
            return out;
        }

        private void resize() {
            int[] oldKeys = keys;
            String[] oldValues = values;
            keys = new int[oldKeys.length << 1];
            values = new String[oldValues.length << 1];
            threshold = (int) (keys.length * MAX_LOAD);
            size = 0;

            for (int i = 0; i < oldKeys.length; i++) {
                int key = oldKeys[i];
                if (key == 0) {
                    continue;
                }
                int slot = findSlot(key, keys);
                keys[slot] = key;
                values[slot] = oldValues[i];
                size++;
            }
        }

        private static int findSlot(int key, int[] table) {
            int mask = table.length - 1;
            int slot = mix(key) & mask;
            while (table[slot] != 0 && table[slot] != key) {
                slot = (slot + 1) & mask;
            }
            return slot;
        }

        private static int mix(int value) {
            int x = value;
            x ^= (x >>> 16);
            x *= 0x7feb352d;
            x ^= (x >>> 15);
            x *= 0x846ca68b;
            x ^= (x >>> 16);
            return x;
        }
    }
}
