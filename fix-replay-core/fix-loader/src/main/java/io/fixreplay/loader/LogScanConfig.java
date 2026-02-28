package io.fixreplay.loader;

import java.util.List;
import java.util.Objects;

public record LogScanConfig(int chunkSize, int maxMessageLength, List<DelimiterRule> delimiterRules) {
    private static final int DEFAULT_CHUNK_SIZE = 16 * 1024;
    private static final int DEFAULT_MAX_MESSAGE_LENGTH = 256 * 1024;

    public LogScanConfig {
        if (chunkSize < 16) {
            throw new IllegalArgumentException("chunkSize must be >= 16");
        }
        if (maxMessageLength < 64) {
            throw new IllegalArgumentException("maxMessageLength must be >= 64");
        }
        if (maxMessageLength < chunkSize) {
            throw new IllegalArgumentException("maxMessageLength must be >= chunkSize");
        }
        delimiterRules = List.copyOf(Objects.requireNonNull(delimiterRules, "delimiterRules"));
        if (delimiterRules.isEmpty()) {
            throw new IllegalArgumentException("delimiterRules must not be empty");
        }
    }

    public static LogScanConfig defaults() {
        return new LogScanConfig(
            DEFAULT_CHUNK_SIZE,
            DEFAULT_MAX_MESSAGE_LENGTH,
            List.of(DelimiterRule.SOH, DelimiterRule.CARET_A, DelimiterRule.PIPE)
        );
    }

    public boolean supports(DelimiterRule rule) {
        return delimiterRules.contains(rule);
    }

    public enum DelimiterRule {
        SOH,
        CARET_A,
        PIPE
    }
}
