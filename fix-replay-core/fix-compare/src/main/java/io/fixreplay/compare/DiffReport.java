package io.fixreplay.compare;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class DiffReport {
    private final List<MessageResult> messages;

    public DiffReport(List<MessageResult> messages) {
        this.messages = List.copyOf(Objects.requireNonNull(messages, "messages"));
    }

    public static DiffReport fromCompareResults(List<CompareResult> results) {
        List<MessageResult> items = new ArrayList<>(results.size());
        for (int i = 0; i < results.size(); i++) {
            items.add(new MessageResult("message-" + (i + 1), results.get(i)));
        }
        return new DiffReport(items);
    }

    public List<MessageResult> messages() {
        return messages;
    }

    public int totalMessages() {
        return messages.size();
    }

    public long failedMessages() {
        return messages.stream().filter(entry -> !entry.result().passed()).count();
    }

    public String toJson() {
        StringBuilder out = new StringBuilder(512);
        out.append('{');
        out.append("\"totalMessages\":").append(totalMessages()).append(',');
        out.append("\"failedMessages\":").append(failedMessages()).append(',');
        out.append("\"messages\":[");
        for (int i = 0; i < messages.size(); i++) {
            if (i > 0) {
                out.append(',');
            }
            MessageResult message = messages.get(i);
            CompareResult result = message.result();
            out.append('{');
            out.append("\"id\":\"").append(escape(message.id())).append("\",");
            out.append("\"msgType\":").append(stringOrNull(result.msgType())).append(',');
            out.append("\"passed\":").append(result.passed()).append(',');
            out.append("\"missingTags\":").append(intArray(result.missingTags())).append(',');
            out.append("\"extraTags\":").append(intArray(result.extraTags())).append(',');
            out.append("\"differingValues\":{");
            int index = 0;
            for (var entry : result.differingValues().entrySet()) {
                if (index++ > 0) {
                    out.append(',');
                }
                out.append('"').append(entry.getKey()).append('"').append(':');
                out.append('{');
                out.append("\"expected\":").append(stringOrNull(entry.getValue().expected())).append(',');
                out.append("\"actual\":").append(stringOrNull(entry.getValue().actual()));
                out.append('}');
            }
            out.append('}');
            out.append('}');
        }
        out.append("]}");
        return out.toString();
    }

    public String toJUnitXml() {
        return toJUnitXml("fix-compare");
    }

    public String toJUnitXml(String suiteName) {
        StringBuilder out = new StringBuilder(1024);
        long failures = failedMessages();
        out.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        out.append("<testsuite name=\"").append(escapeXml(suiteName)).append("\"");
        out.append(" tests=\"").append(messages.size()).append("\"");
        out.append(" failures=\"").append(failures).append("\"");
        out.append(" errors=\"0\" skipped=\"0\">");

        for (MessageResult message : messages) {
            CompareResult result = message.result();
            out.append("<testcase name=\"").append(escapeXml(message.id())).append("\"");
            if (result.msgType() != null) {
                out.append(" classname=\"msgType.").append(escapeXml(result.msgType())).append("\"");
            }
            out.append(">");
            if (!result.passed()) {
                out.append("<failure message=\"Comparison failed\">");
                out.append(escapeXml(summarizeFailure(result)));
                out.append("</failure>");
            }
            out.append("</testcase>");
        }

        out.append("</testsuite>");
        return out.toString();
    }

    private static String summarizeFailure(CompareResult result) {
        StringBuilder out = new StringBuilder();
        if (!result.missingTags().isEmpty()) {
            out.append("missing=").append(result.missingTags()).append("; ");
        }
        if (!result.extraTags().isEmpty()) {
            out.append("extra=").append(result.extraTags()).append("; ");
        }
        if (!result.differingValues().isEmpty()) {
            out.append("diff=").append(result.differingValues());
        }
        return out.toString().trim();
    }

    private static String intArray(List<Integer> tags) {
        StringBuilder out = new StringBuilder();
        out.append('[');
        for (int i = 0; i < tags.size(); i++) {
            if (i > 0) {
                out.append(',');
            }
            out.append(tags.get(i));
        }
        out.append(']');
        return out.toString();
    }

    private static String stringOrNull(String value) {
        return value == null ? "null" : "\"" + escape(value) + "\"";
    }

    private static String escape(String value) {
        StringBuilder out = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\':
                    out.append("\\\\");
                    break;
                case '"':
                    out.append("\\\"");
                    break;
                case '\n':
                    out.append("\\n");
                    break;
                case '\r':
                    out.append("\\r");
                    break;
                case '\t':
                    out.append("\\t");
                    break;
                default:
                    out.append(c);
            }
        }
        return out.toString();
    }

    private static String escapeXml(String value) {
        StringBuilder out = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '&':
                    out.append("&amp;");
                    break;
                case '<':
                    out.append("&lt;");
                    break;
                case '>':
                    out.append("&gt;");
                    break;
                case '"':
                    out.append("&quot;");
                    break;
                case '\'':
                    out.append("&apos;");
                    break;
                default:
                    out.append(c);
            }
        }
        return out.toString();
    }

    public record MessageResult(String id, CompareResult result) {
        public MessageResult {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(result, "result");
        }
    }
}
