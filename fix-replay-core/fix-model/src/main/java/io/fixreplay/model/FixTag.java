package io.fixreplay.model;

public record FixTag(int tag, String value) {
    public static FixTag parse(String token) {
        int index = token.indexOf('=');
        if (index < 1 || index >= token.length() - 1) {
            throw new IllegalArgumentException("Invalid FIX token: " + token);
        }
        return new FixTag(Integer.parseInt(token.substring(0, index)), token.substring(index + 1));
    }
}
