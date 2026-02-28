package io.fixreplay.linker;

import java.util.List;
import java.util.Objects;

public record LinkKey(String msgType, List<Integer> tags, List<String> keyParts) {
    public LinkKey {
        msgType = Objects.requireNonNull(msgType, "msgType");
        tags = List.copyOf(Objects.requireNonNull(tags, "tags"));
        keyParts = List.copyOf(Objects.requireNonNull(keyParts, "keyParts"));
        if (tags.isEmpty()) {
            throw new IllegalArgumentException("tags must not be empty");
        }
        if (tags.size() != keyParts.size()) {
            throw new IllegalArgumentException("tags and keyParts size mismatch");
        }
    }

    public String asCorrelationId() {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < tags.size(); i++) {
            if (i > 0) {
                out.append('|');
            }
            out.append(tags.get(i)).append('=').append(keyParts.get(i));
        }
        return out.toString();
    }
}
