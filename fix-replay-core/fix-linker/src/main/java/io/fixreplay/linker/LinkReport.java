package io.fixreplay.linker;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class LinkReport {
    private final Map<String, List<Integer>> strategyByMsgType;
    private final List<FixLink> matches;
    private final int unmatched;
    private final int ambiguous;
    private final List<CollisionExample> topCollisions;

    public LinkReport(
        Map<String, List<Integer>> strategyByMsgType,
        List<FixLink> matches,
        int unmatched,
        int ambiguous,
        List<CollisionExample> topCollisions
    ) {
        this.strategyByMsgType = freezeStrategies(strategyByMsgType);
        this.matches = List.copyOf(matches);
        this.unmatched = unmatched;
        this.ambiguous = ambiguous;
        this.topCollisions = List.copyOf(topCollisions);
    }

    public Map<String, List<Integer>> strategyByMsgType() {
        return strategyByMsgType;
    }

    public List<FixLink> matches() {
        return matches;
    }

    public int matched() {
        return matches.size();
    }

    public int unmatched() {
        return unmatched;
    }

    public int ambiguous() {
        return ambiguous;
    }

    public List<CollisionExample> topCollisions() {
        return topCollisions;
    }

    public String toJson() {
        StringBuilder out = new StringBuilder(512);
        out.append('{');

        out.append("\"strategyByMsgType\":{");
        boolean firstType = true;
        for (Map.Entry<String, List<Integer>> entry : new TreeMap<>(strategyByMsgType).entrySet()) {
            if (!firstType) {
                out.append(',');
            }
            firstType = false;
            out.append('"').append(escape(entry.getKey())).append('"').append(':');
            out.append('[');
            for (int i = 0; i < entry.getValue().size(); i++) {
                if (i > 0) {
                    out.append(',');
                }
                out.append(entry.getValue().get(i));
            }
            out.append(']');
        }
        out.append('}');

        out.append(",\"counts\":{");
        out.append("\"matched\":").append(matched()).append(',');
        out.append("\"unmatched\":").append(unmatched).append(',');
        out.append("\"ambiguous\":").append(ambiguous);
        out.append('}');

        out.append(",\"topCollisions\":[");
        for (int i = 0; i < topCollisions.size(); i++) {
            CollisionExample collision = topCollisions.get(i);
            if (i > 0) {
                out.append(',');
            }
            out.append('{');
            out.append("\"msgType\":\"").append(escape(collision.msgType())).append("\",");
            out.append("\"tags\":").append(asIntArray(collision.tags())).append(',');
            out.append("\"key\":\"").append(escape(collision.key())).append("\",");
            out.append("\"inCount\":").append(collision.inCount()).append(',');
            out.append("\"inLines\":").append(asLongArray(collision.inLines()));
            out.append('}');
        }
        out.append(']');

        out.append('}');
        return out.toString();
    }

    private static Map<String, List<Integer>> freezeStrategies(Map<String, List<Integer>> source) {
        Map<String, List<Integer>> out = new TreeMap<>();
        for (Map.Entry<String, List<Integer>> entry : source.entrySet()) {
            out.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return Map.copyOf(out);
    }

    private static String asIntArray(List<Integer> values) {
        StringBuilder out = new StringBuilder();
        out.append('[');
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                out.append(',');
            }
            out.append(values.get(i));
        }
        out.append(']');
        return out.toString();
    }

    private static String asLongArray(List<Long> values) {
        StringBuilder out = new StringBuilder();
        out.append('[');
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                out.append(',');
            }
            out.append(values.get(i));
        }
        out.append(']');
        return out.toString();
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

    public record CollisionExample(String msgType, List<Integer> tags, String key, int inCount, List<Long> inLines) {
        public CollisionExample {
            tags = List.copyOf(tags);
            inLines = List.copyOf(inLines);
        }
    }
}
