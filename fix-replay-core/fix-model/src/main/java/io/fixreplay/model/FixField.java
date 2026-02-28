package io.fixreplay.model;

public record FixField(int tag, String value) {
    public FixField {
        if (tag <= 0) {
            throw new IllegalArgumentException("FIX tag must be positive: " + tag);
        }
    }
}
