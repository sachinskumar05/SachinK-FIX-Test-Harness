package io.fixreplay.runner;

import java.util.Objects;

public record SessionKey(String senderCompId, String targetCompId) {
    public SessionKey {
        senderCompId = Objects.requireNonNull(senderCompId, "senderCompId");
        targetCompId = Objects.requireNonNull(targetCompId, "targetCompId");
    }

    public String id() {
        return senderCompId + "_" + targetCompId;
    }
}
