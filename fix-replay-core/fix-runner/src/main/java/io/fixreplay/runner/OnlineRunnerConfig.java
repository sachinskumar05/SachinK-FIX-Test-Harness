package io.fixreplay.runner;

import io.fixreplay.compare.CompareConfig;
import io.fixreplay.linker.LinkerConfig;
import io.fixreplay.model.MsgTypeFilter;
import java.time.Duration;
import java.util.Objects;

public record OnlineRunnerConfig(
    LinkerConfig linkerConfig,
    CompareConfig compareConfig,
    MsgTypeFilter msgTypeFilter,
    Duration receiveTimeout,
    int queueCapacity
) {
    public OnlineRunnerConfig {
        linkerConfig = Objects.requireNonNull(linkerConfig, "linkerConfig");
        compareConfig = Objects.requireNonNull(compareConfig, "compareConfig");
        msgTypeFilter = Objects.requireNonNull(msgTypeFilter, "msgTypeFilter");
        receiveTimeout = Objects.requireNonNull(receiveTimeout, "receiveTimeout");
        if (receiveTimeout.isZero() || receiveTimeout.isNegative()) {
            throw new IllegalArgumentException("receiveTimeout must be > 0");
        }
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be > 0");
        }
    }

    public static OnlineRunnerConfig defaults() {
        return new OnlineRunnerConfig(
            LinkerConfig.defaults(),
            CompareConfig.defaults(),
            new MsgTypeFilter(),
            Duration.ofSeconds(5),
            1024
        );
    }
}
