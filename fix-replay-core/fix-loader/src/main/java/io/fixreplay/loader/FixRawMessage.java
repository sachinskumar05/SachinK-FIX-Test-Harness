package io.fixreplay.loader;

import io.fixreplay.model.FixMessage;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

public final class FixRawMessage {
    private final Path sourceFile;
    private final long offset;
    private final byte[] payload;
    private final Optional<String> timestamp;
    private final Optional<Direction> direction;

    public FixRawMessage(
        Path sourceFile,
        long offset,
        byte[] payload,
        Optional<String> timestamp,
        Optional<Direction> direction
    ) {
        this.sourceFile = Objects.requireNonNull(sourceFile, "sourceFile");
        this.offset = offset;
        this.payload = Arrays.copyOf(Objects.requireNonNull(payload, "payload"), payload.length);
        this.timestamp = Objects.requireNonNullElse(timestamp, Optional.empty());
        this.direction = Objects.requireNonNullElse(direction, Optional.empty());
    }

    public Path sourceFile() {
        return sourceFile;
    }

    public long offset() {
        return offset;
    }

    public byte[] payload() {
        return Arrays.copyOf(payload, payload.length);
    }

    public String payloadText() {
        return new String(payload, StandardCharsets.ISO_8859_1);
    }

    public Optional<String> timestamp() {
        return timestamp;
    }

    public Optional<Direction> direction() {
        return direction;
    }

    public FixMessage toFixMessage() {
        return FixMessage.fromRaw(payloadText(), '\u0001');
    }

    @Override
    public String toString() {
        return "FixRawMessage{" +
            "sourceFile=" + sourceFile +
            ", offset=" + offset +
            ", payloadLength=" + payload.length +
            ", timestamp=" + timestamp +
            ", direction=" + direction +
            '}';
    }

    public enum Direction {
        IN,
        OUT
    }
}
