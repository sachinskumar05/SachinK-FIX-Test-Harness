package io.fixreplay.runner;

import java.util.Map;
import java.util.Objects;

public record TransportSessionConfig(
    SessionKey entrySession,
    SessionKey exitSession,
    Map<String, String> properties
) {
    public TransportSessionConfig {
        entrySession = Objects.requireNonNull(entrySession, "entrySession");
        exitSession = Objects.requireNonNull(exitSession, "exitSession");
        properties = Map.copyOf(Objects.requireNonNullElse(properties, Map.of()));
    }

    public static TransportSessionConfig of(SessionKey entrySession, SessionKey exitSession) {
        return new TransportSessionConfig(entrySession, exitSession, Map.of());
    }
}
