package io.fixreplay.model;

import java.util.Objects;
import java.util.Set;

public final class MsgTypeFilter {
    private static final Set<String> DEFAULT_ALLOWED = Set.of("D", "G", "F", "8", "3", "j");

    private final Set<String> allowed;

    public MsgTypeFilter() {
        this(DEFAULT_ALLOWED);
    }

    public MsgTypeFilter(Set<String> allowed) {
        this.allowed = Set.copyOf(Objects.requireNonNull(allowed, "allowed"));
    }

    public boolean accepts(FixMessage message) {
        return accepts(message.msgType());
    }

    public boolean accepts(String msgType) {
        return msgType != null && allowed.contains(msgType);
    }

    public Set<String> allowed() {
        return allowed;
    }
}
