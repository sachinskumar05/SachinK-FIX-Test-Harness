package io.fixreplay.loader;

import io.fixreplay.model.FixMessage;

public record FixLogEntry(long lineNumber, FixMessage message) {
}
