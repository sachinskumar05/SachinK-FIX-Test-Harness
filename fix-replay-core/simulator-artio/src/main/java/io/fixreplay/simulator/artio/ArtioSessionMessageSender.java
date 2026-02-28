package io.fixreplay.simulator.artio;

import io.fixreplay.model.FixMessage;
import io.fixreplay.model.FixParser;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.builder.HeaderEncoder;
import uk.co.real_logic.artio.session.CompositeKey;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.util.MessageTypeEncoding;

final class ArtioSessionMessageSender {
    private static final char SOH = '\u0001';
    private static final Set<Integer> CONTROLLED_TAGS = Set.of(8, 9, 10, 34, 52, 49, 56);
    private static final int DEFAULT_MAX_OUTBOUND_LENGTH = 128 * 1024;
    private static final int DEFAULT_MAX_SEND_ATTEMPTS = 3;
    private static final long DEFAULT_RETRY_IDLE_NANOS = 1_000_000L;

    private final int maxOutboundLength;
    private final int maxSendAttempts;
    private final long retryIdleNanos;

    ArtioSessionMessageSender() {
        this(DEFAULT_MAX_OUTBOUND_LENGTH, DEFAULT_MAX_SEND_ATTEMPTS, DEFAULT_RETRY_IDLE_NANOS);
    }

    ArtioSessionMessageSender(int maxOutboundLength, int maxSendAttempts, long retryIdleNanos) {
        if (maxOutboundLength <= 0) {
            throw new IllegalArgumentException("maxOutboundLength must be > 0");
        }
        if (maxSendAttempts <= 0) {
            throw new IllegalArgumentException("maxSendAttempts must be > 0");
        }
        if (retryIdleNanos < 0) {
            throw new IllegalArgumentException("retryIdleNanos must be >= 0");
        }
        this.maxOutboundLength = maxOutboundLength;
        this.maxSendAttempts = maxSendAttempts;
        this.retryIdleNanos = retryIdleNanos;
    }

    long sendOnSession(Session session, FixMessage message, String beginString) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(message, "message");
        String resolvedBeginString = requireNonBlank(beginString, "beginString");

        String msgType = requireNonBlank(message.msgType(), "tag 35 (MsgType)");
        PreparedHeaderState headerState = prepareHeader(session, message, resolvedBeginString, msgType);
        byte[] encodedPayload = encodePayload(message, resolvedBeginString, headerState, maxOutboundLength);
        UnsafeBuffer outboundBuffer = new UnsafeBuffer(encodedPayload);
        long packedType = MessageTypeEncoding.packMessageType(msgType);

        long result = Long.MIN_VALUE;
        for (int attempt = 1; attempt <= maxSendAttempts; attempt++) {
            result = session.trySend(
                outboundBuffer,
                0,
                encodedPayload.length,
                headerState.msgSeqNum(),
                packedType
            );
            if (result > 0) {
                return result;
            }
            if (attempt < maxSendAttempts && retryIdleNanos > 0) {
                LockSupport.parkNanos(retryIdleNanos);
            }
        }
        return result;
    }

    static FixMessage toFixMessage(Map<Integer, String> fields) {
        Objects.requireNonNull(fields, "fields");
        if (!fields.containsKey(35)) {
            throw new IllegalArgumentException("Mutated fields must contain tag 35 (MsgType)");
        }

        List<Map.Entry<Integer, String>> ordered = new ArrayList<>(fields.entrySet());
        ordered.sort(Comparator.comparingInt(Map.Entry::getKey));

        StringBuilder canonical = new StringBuilder(Math.max(64, ordered.size() * 16));
        for (Map.Entry<Integer, String> entry : ordered) {
            Integer tag = entry.getKey();
            String value = entry.getValue();
            if (tag == null || tag <= 0 || value == null) {
                continue;
            }
            canonical.append(tag).append('=').append(value).append(SOH);
        }
        return FixParser.parse(canonical.toString());
    }

    static byte[] encodePayload(
        FixMessage message,
        String beginString,
        PreparedHeaderState headerState,
        int maxOutboundLength
    ) {
        Objects.requireNonNull(message, "message");
        String resolvedBeginString = requireNonBlank(beginString, "beginString");
        Objects.requireNonNull(headerState, "headerState");

        List<Map.Entry<Integer, String>> businessFields = new ArrayList<>(message.fields().entrySet());
        businessFields.sort(Comparator.comparingInt(Map.Entry::getKey));

        StringBuilder body = new StringBuilder(256);
        appendField(body, 35, headerState.msgType());
        appendField(body, 49, headerState.senderCompId());
        appendField(body, 56, headerState.targetCompId());
        appendField(body, 34, Integer.toString(headerState.msgSeqNum()));
        appendField(body, 52, headerState.sendingTime());
        for (Map.Entry<Integer, String> entry : businessFields) {
            Integer tag = entry.getKey();
            String value = entry.getValue();
            if (tag == null || tag <= 0 || value == null || tag == 35 || CONTROLLED_TAGS.contains(tag)) {
                continue;
            }
            appendField(body, tag, value);
        }

        byte[] bodyBytes = body.toString().getBytes(StandardCharsets.ISO_8859_1);
        StringBuilder payloadWithoutChecksum = new StringBuilder(bodyBytes.length + 48);
        appendField(payloadWithoutChecksum, 8, resolvedBeginString);
        appendField(payloadWithoutChecksum, 9, Integer.toString(bodyBytes.length));
        payloadWithoutChecksum.append(body);

        int checksum = checksum(payloadWithoutChecksum);
        StringBuilder payload = new StringBuilder(payloadWithoutChecksum.length() + 8);
        payload.append(payloadWithoutChecksum);
        payload.append("10=");
        if (checksum < 100) {
            payload.append('0');
        }
        if (checksum < 10) {
            payload.append('0');
        }
        payload.append(checksum).append(SOH);

        byte[] encoded = payload.toString().getBytes(StandardCharsets.ISO_8859_1);
        if (encoded.length > maxOutboundLength) {
            throw new IllegalArgumentException(
                "Encoded outbound FIX message length " + encoded.length + " exceeds max " + maxOutboundLength
            );
        }
        return encoded;
    }

    private static PreparedHeaderState prepareHeader(
        Session session,
        FixMessage message,
        String beginString,
        String msgType
    ) {
        HeaderEncoder headerEncoder = new HeaderEncoder();
        headerEncoder.beginString(beginString);
        headerEncoder.msgType(msgType);
        int preparedSeqNum = session.prepare(headerEncoder);
        int msgSeqNum = headerEncoder.hasMsgSeqNum() ? headerEncoder.msgSeqNum() : preparedSeqNum;
        if (msgSeqNum <= 0) {
            throw new IllegalStateException("Artio session.prepare did not produce a valid MsgSeqNum");
        }

        CompositeKey key = session.compositeKey();
        String senderCompId = firstNonBlank(
            headerEncoder.hasSenderCompID() ? headerEncoder.senderCompIDAsString() : null,
            key == null ? null : key.localCompId(),
            message.senderCompId()
        );
        String targetCompId = firstNonBlank(
            headerEncoder.hasTargetCompID() ? headerEncoder.targetCompIDAsString() : null,
            key == null ? null : key.remoteCompId(),
            message.targetCompId()
        );
        String sendingTime = firstNonBlank(
            headerEncoder.hasSendingTime() ? headerEncoder.sendingTimeAsString() : null,
            message.getString(52)
        );

        return new PreparedHeaderState(
            msgSeqNum,
            requireNonBlank(senderCompId, "SenderCompID"),
            requireNonBlank(targetCompId, "TargetCompID"),
            requireNonBlank(sendingTime, "SendingTime"),
            msgType
        );
    }

    private static String firstNonBlank(String... candidates) {
        for (String candidate : candidates) {
            if (candidate != null && !candidate.isBlank()) {
                return candidate;
            }
        }
        return null;
    }

    private static void appendField(StringBuilder builder, int tag, String value) {
        if (value == null) {
            return;
        }
        builder.append(tag).append('=').append(value).append(SOH);
    }

    private static int checksum(CharSequence payload) {
        int sum = 0;
        for (int i = 0; i < payload.length(); i++) {
            sum += payload.charAt(i) & 0xFF;
        }
        return sum % 256;
    }

    private static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    static String toDisplayString(byte[] payload) {
        return new String(payload, StandardCharsets.ISO_8859_1).replace(SOH, '|');
    }

    record PreparedHeaderState(
        int msgSeqNum,
        String senderCompId,
        String targetCompId,
        String sendingTime,
        String msgType
    ) {
        PreparedHeaderState {
            if (msgSeqNum <= 0) {
                throw new IllegalArgumentException("msgSeqNum must be > 0");
            }
            senderCompId = requireNonBlank(senderCompId, "senderCompId");
            targetCompId = requireNonBlank(targetCompId, "targetCompId");
            sendingTime = requireNonBlank(sendingTime, "sendingTime");
            msgType = requireNonBlank(msgType, "msgType");
        }
    }
}
