package io.fixreplay.linker;

import io.fixreplay.loader.FixLogEntry;
import io.fixreplay.model.FixMessage;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class LinkIndex {
    private final LinkerConfig config;
    private final Map<String, List<Integer>> strategyByMsgType;
    private final Map<LinkKey, List<FixLogEntry>> inIndex;

    private LinkIndex(LinkerConfig config, Map<String, List<Integer>> strategyByMsgType, Map<LinkKey, List<FixLogEntry>> inIndex) {
        this.config = config;
        this.strategyByMsgType = strategyByMsgType;
        this.inIndex = inIndex;
    }

    public static LinkIndex build(
        List<FixLogEntry> inMessages,
        Map<String, List<Integer>> strategyByMsgType,
        LinkerConfig config
    ) {
        Objects.requireNonNull(inMessages, "inMessages");
        Map<String, List<Integer>> strategy = Map.copyOf(strategyByMsgType);
        Map<LinkKey, List<FixLogEntry>> index = new HashMap<>();

        for (FixLogEntry entry : inMessages) {
            String msgType = entry.message().msgType();
            if (msgType == null) {
                continue;
            }
            List<Integer> tags = strategy.get(msgType);
            if (tags == null || tags.isEmpty()) {
                continue;
            }
            LinkKey key = buildKey(config, msgType, entry.message(), tags);
            if (key == null) {
                continue;
            }
            index.computeIfAbsent(key, ignored -> new ArrayList<>()).add(entry);
        }
        return new LinkIndex(config, strategy, index);
    }

    public Result link(List<FixLogEntry> outMessages) {
        List<FixLink> matches = new ArrayList<>();
        int unmatched = 0;
        int ambiguous = 0;

        for (FixLogEntry out : outMessages) {
            String msgType = out.message().msgType();
            List<Integer> tags = msgType == null ? null : strategyByMsgType.get(msgType);
            if (msgType == null || tags == null || tags.isEmpty()) {
                unmatched++;
                continue;
            }

            LinkKey key = buildKey(config, msgType, out.message(), tags);
            if (key == null) {
                unmatched++;
                continue;
            }

            List<FixLogEntry> candidates = inIndex.get(key);
            if (candidates == null || candidates.isEmpty()) {
                unmatched++;
                continue;
            }
            if (candidates.size() > 1) {
                ambiguous++;
                continue;
            }

            FixLogEntry inMatch = candidates.get(0);
            matches.add(new FixLink(key.asCorrelationId(), inMatch.lineNumber(), out.lineNumber()));
        }

        List<LinkReport.CollisionExample> topCollisions = collectCollisionExamples();
        return new Result(List.copyOf(matches), unmatched, ambiguous, topCollisions);
    }

    private List<LinkReport.CollisionExample> collectCollisionExamples() {
        List<LinkReport.CollisionExample> collisions = new ArrayList<>();
        for (Map.Entry<LinkKey, List<FixLogEntry>> entry : inIndex.entrySet()) {
            if (entry.getValue().size() < 2) {
                continue;
            }
            List<Long> inLines = entry.getValue().stream().map(FixLogEntry::lineNumber).limit(5).toList();
            collisions.add(
                new LinkReport.CollisionExample(
                    entry.getKey().msgType(),
                    entry.getKey().tags(),
                    entry.getKey().asCorrelationId(),
                    entry.getValue().size(),
                    inLines
                )
            );
        }

        collisions.sort(
            Comparator.comparingInt(LinkReport.CollisionExample::inCount).reversed()
                .thenComparing(LinkReport.CollisionExample::msgType)
                .thenComparing(LinkReport.CollisionExample::key)
        );
        if (collisions.size() <= 5) {
            return List.copyOf(collisions);
        }
        return List.copyOf(collisions.subList(0, 5));
    }

    private static LinkKey buildKey(LinkerConfig config, String msgType, FixMessage message, List<Integer> tags) {
        List<String> keyParts = new ArrayList<>(tags.size());
        for (Integer tag : tags) {
            String value = config.normalizeValue(tag, message.get(tag));
            if (value == null || value.isEmpty()) {
                return null;
            }
            keyParts.add(value);
        }
        return new LinkKey(msgType, tags, keyParts);
    }

    public record Result(List<FixLink> matches, int unmatched, int ambiguous, List<LinkReport.CollisionExample> topCollisions) {
        public int matched() {
            return matches.size();
        }
    }
}
