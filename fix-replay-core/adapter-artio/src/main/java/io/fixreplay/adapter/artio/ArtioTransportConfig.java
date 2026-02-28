package io.fixreplay.adapter.artio;

import io.fixreplay.runner.TransportSessionConfig;
import java.util.Map;
import java.util.Objects;

record ArtioTransportConfig(
    String host,
    int port,
    String beginString,
    String senderCompId,
    String targetCompId,
    String exitSenderCompId,
    String exitTargetCompId,
    String username,
    String password,
    int heartbeatIntervalSeconds,
    long replyTimeoutMs,
    long connectTimeoutMs,
    int pollFragmentLimit,
    long pollIdleMicros,
    boolean sequenceNumbersPersistent,
    boolean resetSeqNum,
    boolean disconnectOnFirstMessageNotLogon,
    String aeronChannel,
    String aeronDir
) {
    static final String HOST_PROPERTY = "artio.host";
    static final String PORT_PROPERTY = "artio.port";
    static final String EXIT_HOST_PROPERTY = "artio.exitHost";
    static final String EXIT_PORT_PROPERTY = "artio.exitPort";
    static final String BEGIN_STRING_PROPERTY = "artio.beginString";
    static final String SENDER_COMP_ID_PROPERTY = "artio.senderCompId";
    static final String TARGET_COMP_ID_PROPERTY = "artio.targetCompId";
    static final String EXIT_SENDER_COMP_ID_PROPERTY = "artio.exitSenderCompId";
    static final String EXIT_TARGET_COMP_ID_PROPERTY = "artio.exitTargetCompId";
    static final String USERNAME_PROPERTY = "artio.username";
    static final String PASSWORD_PROPERTY = "artio.password";
    static final String HEARTBEAT_SECONDS_PROPERTY = "artio.heartbeatSeconds";
    static final String REPLY_TIMEOUT_MS_PROPERTY = "artio.replyTimeoutMs";
    static final String CONNECT_TIMEOUT_MS_PROPERTY = "artio.connectTimeoutMs";
    static final String POLL_FRAGMENT_LIMIT_PROPERTY = "artio.pollFragmentLimit";
    static final String POLL_IDLE_MICROS_PROPERTY = "artio.pollIdleMicros";
    static final String SEQUENCE_NUMBERS_PERSISTENT_PROPERTY = "artio.sequenceNumbersPersistent";
    static final String RESET_SEQ_NUM_PROPERTY = "artio.resetSeqNum";
    static final String DISCONNECT_ON_FIRST_NON_LOGON_PROPERTY = "artio.disconnectOnFirstMessageNotLogon";
    static final String AERON_CHANNEL_PROPERTY = "artio.aeronChannel";
    static final String AERON_DIR_PROPERTY = "artio.aeronDir";
    static final String EXIT_AERON_DIR_PROPERTY = "artio.exitAeronDir";

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 9999;
    private static final String DEFAULT_BEGIN_STRING = "FIX.4.2";
    private static final int DEFAULT_HEARTBEAT_SECONDS = 30;
    private static final long DEFAULT_REPLY_TIMEOUT_MS = 5_000L;
    private static final long DEFAULT_CONNECT_TIMEOUT_MS = 10_000L;
    private static final int DEFAULT_POLL_FRAGMENT_LIMIT = 20;
    private static final long DEFAULT_POLL_IDLE_MICROS = 200L;
    private static final String DEFAULT_AERON_CHANNEL = "aeron:ipc";

    ArtioTransportConfig {
        host = requireNonBlank(host, HOST_PROPERTY);
        beginString = requireNonBlank(beginString, BEGIN_STRING_PROPERTY);
        senderCompId = requireNonBlank(senderCompId, SENDER_COMP_ID_PROPERTY);
        targetCompId = requireNonBlank(targetCompId, TARGET_COMP_ID_PROPERTY);
        exitSenderCompId = requireNonBlank(exitSenderCompId, EXIT_SENDER_COMP_ID_PROPERTY);
        exitTargetCompId = requireNonBlank(exitTargetCompId, EXIT_TARGET_COMP_ID_PROPERTY);

        if (port <= 0) {
            throw new IllegalArgumentException(PORT_PROPERTY + " must be > 0");
        }
        if (heartbeatIntervalSeconds <= 0) {
            throw new IllegalArgumentException(HEARTBEAT_SECONDS_PROPERTY + " must be > 0");
        }
        if (replyTimeoutMs <= 0) {
            throw new IllegalArgumentException(REPLY_TIMEOUT_MS_PROPERTY + " must be > 0");
        }
        if (connectTimeoutMs <= 0) {
            throw new IllegalArgumentException(CONNECT_TIMEOUT_MS_PROPERTY + " must be > 0");
        }
        if (pollFragmentLimit <= 0) {
            throw new IllegalArgumentException(POLL_FRAGMENT_LIMIT_PROPERTY + " must be > 0");
        }
        if (pollIdleMicros < 0) {
            throw new IllegalArgumentException(POLL_IDLE_MICROS_PROPERTY + " must be >= 0");
        }
    }

    static ArtioTransportConfig from(TransportSessionConfig sessionConfig) {
        Objects.requireNonNull(sessionConfig, "sessionConfig");
        Map<String, String> properties = sessionConfig.properties();

        String host = readString(properties, HOST_PROPERTY, DEFAULT_HOST);
        int port = readInt(properties, PORT_PROPERTY, DEFAULT_PORT);
        String beginString = readString(properties, BEGIN_STRING_PROPERTY, DEFAULT_BEGIN_STRING);
        String senderCompId = readString(properties, SENDER_COMP_ID_PROPERTY, sessionConfig.entrySession().senderCompId());
        String targetCompId = readString(properties, TARGET_COMP_ID_PROPERTY, sessionConfig.entrySession().targetCompId());
        String exitSenderCompId = readString(
            properties,
            EXIT_SENDER_COMP_ID_PROPERTY,
            sessionConfig.exitSession().senderCompId()
        );
        String exitTargetCompId = readString(
            properties,
            EXIT_TARGET_COMP_ID_PROPERTY,
            sessionConfig.exitSession().targetCompId()
        );
        String username = readOptionalString(properties, USERNAME_PROPERTY);
        String password = readOptionalString(properties, PASSWORD_PROPERTY);
        int heartbeatIntervalSeconds = readInt(properties, HEARTBEAT_SECONDS_PROPERTY, DEFAULT_HEARTBEAT_SECONDS);
        long replyTimeoutMs = readLong(properties, REPLY_TIMEOUT_MS_PROPERTY, DEFAULT_REPLY_TIMEOUT_MS);
        long connectTimeoutMs = readLong(properties, CONNECT_TIMEOUT_MS_PROPERTY, DEFAULT_CONNECT_TIMEOUT_MS);
        int pollFragmentLimit = readInt(properties, POLL_FRAGMENT_LIMIT_PROPERTY, DEFAULT_POLL_FRAGMENT_LIMIT);
        long pollIdleMicros = readLong(properties, POLL_IDLE_MICROS_PROPERTY, DEFAULT_POLL_IDLE_MICROS);
        boolean sequenceNumbersPersistent = readBoolean(properties, SEQUENCE_NUMBERS_PERSISTENT_PROPERTY, false);
        boolean resetSeqNum = readBoolean(properties, RESET_SEQ_NUM_PROPERTY, false);
        boolean disconnectOnFirstMessageNotLogon = readBoolean(
            properties,
            DISCONNECT_ON_FIRST_NON_LOGON_PROPERTY,
            true
        );
        String aeronChannel = readString(properties, AERON_CHANNEL_PROPERTY, DEFAULT_AERON_CHANNEL);
        String aeronDir = readOptionalString(properties, AERON_DIR_PROPERTY);

        return new ArtioTransportConfig(
            host,
            port,
            beginString,
            senderCompId,
            targetCompId,
            exitSenderCompId,
            exitTargetCompId,
            username,
            password,
            heartbeatIntervalSeconds,
            replyTimeoutMs,
            connectTimeoutMs,
            pollFragmentLimit,
            pollIdleMicros,
            sequenceNumbersPersistent,
            resetSeqNum,
            disconnectOnFirstMessageNotLogon,
            aeronChannel,
            aeronDir
        );
    }

    private static String readString(Map<String, String> properties, String key, String defaultValue) {
        String value = readOptionalString(properties, key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    private static String readOptionalString(Map<String, String> properties, String key) {
        String value = properties.get(key);
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static int readInt(Map<String, String> properties, String key, int defaultValue) {
        String raw = properties.get(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException parseFailure) {
            throw new IllegalArgumentException("Invalid integer property " + key + ": " + raw, parseFailure);
        }
    }

    private static long readLong(Map<String, String> properties, String key, long defaultValue) {
        String raw = properties.get(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException parseFailure) {
            throw new IllegalArgumentException("Invalid long property " + key + ": " + raw, parseFailure);
        }
    }

    private static boolean readBoolean(Map<String, String> properties, String key, boolean defaultValue) {
        String raw = properties.get(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(raw.trim());
    }

    private static String requireNonBlank(String value, String propertyName) {
        String candidate = value == null ? "" : value.trim();
        if (candidate.isEmpty()) {
            throw new IllegalArgumentException(propertyName + " must not be blank");
        }
        return candidate;
    }
}
