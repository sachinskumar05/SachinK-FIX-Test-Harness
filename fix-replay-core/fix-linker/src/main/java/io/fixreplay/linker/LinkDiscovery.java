package io.fixreplay.linker;

import io.fixreplay.loader.FixLogEntry;
import io.fixreplay.model.FixMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

public final class LinkDiscovery {
    private static final int SCALE = 10_000;

    private final LinkerConfig config;

    public LinkDiscovery() {
        this(LinkerConfig.defaults());
    }

    public LinkDiscovery(LinkerConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    public Map<String, List<Integer>> discover(List<FixLogEntry> inMessages, List<FixLogEntry> outMessages) {
        Map<String, List<FixLogEntry>> inByType = groupByMsgType(inMessages);
        Map<String, List<FixLogEntry>> outByType = groupByMsgType(outMessages);

        TreeSet<String> relevantMsgTypes = new TreeSet<>(outByType.keySet());
        Map<String, List<Integer>> chosen = new HashMap<>();

        for (String msgType : relevantMsgTypes) {
            List<FixLogEntry> inSide = inByType.getOrDefault(msgType, List.of());
            List<FixLogEntry> outSide = outByType.getOrDefault(msgType, List.of());
            if (inSide.isEmpty() || outSide.isEmpty()) {
                continue;
            }
            List<Integer> best = chooseBestCandidate(msgType, inSide, outSide);
            if (best != null) {
                chosen.put(msgType, best);
            }
        }
        return Map.copyOf(chosen);
    }

    private List<Integer> chooseBestCandidate(String msgType, List<FixLogEntry> inMessages, List<FixLogEntry> outMessages) {
        CandidateScore best = null;
        for (List<Integer> candidate : config.candidatesFor(msgType)) {
            CandidateScore score = evaluate(msgType, candidate, inMessages, outMessages);
            if (best == null || score.isBetterThan(best)) {
                best = score;
            }
        }
        return best == null ? null : best.tags();
    }

    private CandidateScore evaluate(
        String msgType,
        List<Integer> tags,
        List<FixLogEntry> inMessages,
        List<FixLogEntry> outMessages
    ) {
        Map<LinkKey, Integer> inCounts = new HashMap<>();
        Map<LinkKey, Integer> outCounts = new HashMap<>();

        int validIn = countKeys(msgType, tags, inMessages, inCounts);
        int validOut = countKeys(msgType, tags, outMessages, outCounts);

        int uniquenessIn = ratio(inCounts.size(), validIn);
        int uniquenessOut = ratio(outCounts.size(), validOut);

        int matchedOut = 0;
        for (Map.Entry<LinkKey, Integer> entry : outCounts.entrySet()) {
            if (inCounts.containsKey(entry.getKey())) {
                matchedOut += entry.getValue();
            }
        }
        int matchRate = ratio(matchedOut, validOut);

        int coverageIn = ratio(validIn, inMessages.size());
        int coverageOut = ratio(validOut, outMessages.size());
        int coverage = (coverageIn + coverageOut) / 2;

        int totalScore = uniquenessIn + uniquenessOut + matchRate + coverage;
        return new CandidateScore(tags, totalScore);
    }

    private int countKeys(
        String msgType,
        List<Integer> tags,
        List<FixLogEntry> entries,
        Map<LinkKey, Integer> counts
    ) {
        int valid = 0;
        for (FixLogEntry entry : entries) {
            LinkKey key = keyFor(msgType, entry.message(), tags);
            if (key == null) {
                continue;
            }
            counts.merge(key, 1, Integer::sum);
            valid++;
        }
        return valid;
    }

    private LinkKey keyFor(String msgType, FixMessage message, List<Integer> tags) {
        List<String> parts = new ArrayList<>(tags.size());
        for (Integer tag : tags) {
            String value = config.normalizeValue(tag, message.get(tag));
            if (value == null || value.isEmpty()) {
                return null;
            }
            parts.add(value);
        }
        return new LinkKey(msgType, tags, parts);
    }

    private static Map<String, List<FixLogEntry>> groupByMsgType(List<FixLogEntry> entries) {
        Map<String, List<FixLogEntry>> grouped = new HashMap<>();
        for (FixLogEntry entry : entries) {
            String msgType = entry.message().msgType();
            if (msgType == null || msgType.isEmpty()) {
                continue;
            }
            grouped.computeIfAbsent(msgType, ignored -> new ArrayList<>()).add(entry);
        }
        return grouped;
    }

    private static int ratio(int numerator, int denominator) {
        if (denominator <= 0) {
            return 0;
        }
        return (numerator * SCALE) / denominator;
    }

    private static final class CandidateScore {
        private final List<Integer> tags;
        private final int score;

        private CandidateScore(List<Integer> tags, int score) {
            this.tags = tags;
            this.score = score;
        }

        private List<Integer> tags() {
            return tags;
        }

        private boolean isBetterThan(CandidateScore other) {
            if (score != other.score) {
                return score > other.score;
            }
            if (tags.size() != other.tags.size()) {
                return tags.size() < other.tags.size();
            }
            int size = Math.min(tags.size(), other.tags.size());
            for (int i = 0; i < size; i++) {
                int compare = Integer.compare(tags.get(i), other.tags.get(i));
                if (compare != 0) {
                    return compare < 0;
                }
            }
            return false;
        }
    }
}
