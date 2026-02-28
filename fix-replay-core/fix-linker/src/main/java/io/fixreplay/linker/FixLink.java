package io.fixreplay.linker;

public record FixLink(String correlationId, long requestLine, long responseLine) {
}
