package io.fixreplay.simulator.artio;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import uk.co.real_logic.artio.util.MessageTypeEncoding;

final class ArtioOutboundCodec {
    private static final char SOH = '\u0001';
    private static final String DEFAULT_SENDING_TIME = "19700101-00:00:00.000";

    private ArtioOutboundCodec() {
    }

    static EncodedFixMessage encode(
        Map<Integer, String> fields,
        String beginString,
        String senderCompId,
        String targetCompId,
        int sequenceNumber
    ) {
        Objects.requireNonNull(fields, "fields");

        String msgType = requireTag(fields, 35);
        String sendingTime = firstNonBlank(fields.get(52), DEFAULT_SENDING_TIME);

        List<Map.Entry<Integer, String>> businessFields = new ArrayList<>(Math.max(8, fields.size()));
        for (Map.Entry<Integer, String> field : fields.entrySet()) {
            if (includeInBusinessSection(field.getKey(), field.getValue())) {
                businessFields.add(field);
            }
        }
        businessFields.sort(Comparator.comparingInt(Map.Entry::getKey));

        StringBuilder body = new StringBuilder(128);
        appendField(body, 35, msgType);
        appendField(body, 49, senderCompId);
        appendField(body, 56, targetCompId);
        appendField(body, 34, Integer.toString(sequenceNumber));
        appendField(body, 52, sendingTime);
        for (Map.Entry<Integer, String> field : businessFields) {
            appendField(body, field.getKey(), field.getValue());
        }

        byte[] bodyBytes = body.toString().getBytes(StandardCharsets.ISO_8859_1);
        StringBuilder payloadWithoutChecksum = new StringBuilder(bodyBytes.length + 32);
        appendField(payloadWithoutChecksum, 8, beginString);
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

        return new EncodedFixMessage(
            payload.toString().getBytes(StandardCharsets.ISO_8859_1),
            sequenceNumber,
            msgType,
            MessageTypeEncoding.packMessageType(msgType)
        );
    }

    private static String requireTag(Map<Integer, String> fields, int tag) {
        String value = fields.get(tag);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Message is missing required tag " + tag);
        }
        return value;
    }

    private static String firstNonBlank(String preferred, String fallback) {
        if (preferred != null && !preferred.isBlank()) {
            return preferred;
        }
        return fallback;
    }

    private static boolean includeInBusinessSection(int tag, String value) {
        if (value == null) {
            return false;
        }
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

    record EncodedFixMessage(byte[] payload, int seqNum, String msgType, long packedMsgType) {
        EncodedFixMessage {
            Objects.requireNonNull(payload, "payload");
            Objects.requireNonNull(msgType, "msgType");
        }
    }
}
