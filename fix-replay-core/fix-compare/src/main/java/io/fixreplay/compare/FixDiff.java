package io.fixreplay.compare;

public record FixDiff(int tag, String expected, String actual) {
}
