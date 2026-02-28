package io.fixreplay.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class FixMessage {
    private final Map<Integer, String> fields;

    private FixMessage(Map<Integer, String> fields) {
        this.fields = fields;
    }

    public static FixMessage fromRaw(String raw, char delimiter) {
        String[] tokens = raw.split(java.util.regex.Pattern.quote(String.valueOf(delimiter)));
        Map<Integer, String> fields = new LinkedHashMap<>();
        for (String token : tokens) {
            String trimmed = token.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            FixTag fixTag = FixTag.parse(trimmed);
            fields.put(fixTag.tag(), fixTag.value());
        }
        return new FixMessage(fields);
    }

    public String get(int tag) {
        return fields.get(tag);
    }

    public Map<Integer, String> fields() {
        return Collections.unmodifiableMap(fields);
    }
}
