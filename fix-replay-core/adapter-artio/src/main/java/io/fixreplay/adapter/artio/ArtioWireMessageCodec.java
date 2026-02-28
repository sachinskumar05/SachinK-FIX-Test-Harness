package io.fixreplay.adapter.artio;

import io.fixreplay.model.FixMessage;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import uk.co.real_logic.artio.util.MessageTypeEncoding;

final class ArtioWireMessageCodec {
    private static final char SOH = '\u0001';
    private static final DateTimeFormatter SENDING_TIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private ArtioWireMessageCodec() {
    }

    static EncodedFixMessage encode(FixMessage message, ArtioTransportConfig config, int fallbackSequenceNumber) {
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(config, "config");

        Map<Integer, String> fields = message.fields();
        String beginString = firstNonBlank(fields.get(8), "FIX.4.4");
        String msgType = requireTag(fields, 35);
        String senderCompId = firstNonBlank(fields.get(49), config.senderCompId());
        String targetCompId = firstNonBlank(fields.get(56), config.targetCompId());
        int sequenceNumber = message.getInt(34, fallbackSequenceNumber);
        String sendingTime = firstNonBlank(fields.get(52), SENDING_TIME_FORMATTER.format(Instant.now()));

        List<Map.Entry<Integer, String>> businessFields = new ArrayList<>(Math.max(4, fields.size()));
        for (Map.Entry<Integer, String> entry : fields.entrySet()) {
            if (includeInBusinessSection(entry.getKey())) {
                businessFields.add(entry);
            }
        }
        businessFields.sort(Comparator.comparingInt(Map.Entry::getKey));

        StringBuilder body = new StringBuilder(128);
        appendField(body, 35, msgType);
        appendField(body, 49, senderCompId);
        appendField(body, 56, targetCompId);
        appendField(body, 34, Integer.toString(sequenceNumber));
        appendField(body, 52, sendingTime);
        for (Map.Entry<Integer, String> entry : businessFields) {
            appendField(body, entry.getKey(), entry.getValue());
        }

        byte[] bodyBytes = body.toString().getBytes(StandardCharsets.ISO_8859_1);
        StringBuilder payloadWithoutChecksum = new StringBuilder(bodyBytes.length + 32);
        appendField(payloadWithoutChecksum, 8, beginString);
        appendField(payloadWithoutChecksum, 9, Integer.toString(bodyBytes.length));
        payloadWithoutChecksum.append(body);

        int checksum = checksum(payloadWithoutChecksum);
        StringBuilder payload = new StringBuilder(payloadWithoutChecksum.length() + 10);
        payload.append(payloadWithoutChecksum);
        payload.append("10=");
        if (checksum < 100) {
            payload.append('0');
        }
        if (checksum < 10) {
            payload.append('0');
        }
        payload.append(checksum).append(SOH);

        return new EncodedFixMessage(
            payload.toString().getBytes(StandardCharsets.ISO_8859_1),
            sequenceNumber,
            MessageTypeEncoding.packMessageType(msgType)
        );
    }

    private static String requireTag(Map<Integer, String> fields, int tag) {
        String value = fields.get(tag);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("FIX message is missing required tag " + tag);
        }
        return value;
    }

    private static String firstNonBlank(String preferred, String fallback) {
        if (preferred != null && !preferred.isBlank()) {
            return preferred;
        }
        return fallback;
    }

    private static boolean includeInBusinessSection(int tag) {
        return switch (tag) {
            case 8, 9, 10, 35, 49, 56, 34, 52 -> false;
            default -> true;
        };
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

    record EncodedFixMessage(byte[] payload, int seqNum, long messageType) {
        EncodedFixMessage {
            Objects.requireNonNull(payload, "payload");
        }
    }
}

