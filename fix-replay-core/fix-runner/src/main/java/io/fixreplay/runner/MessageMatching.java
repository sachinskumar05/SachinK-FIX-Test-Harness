package io.fixreplay.runner;

import io.fixreplay.compare.CompareConfig;
import io.fixreplay.compare.CompareResult;
import io.fixreplay.compare.DiffReport;
import io.fixreplay.compare.FixComparator;
import io.fixreplay.loader.FixLogEntry;
import io.fixreplay.model.FixMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class MessageMatching {
    private MessageMatching() {
    }

    static MatchResult matchAndCompare(
        List<FixLogEntry> expectedOut,
        List<FixLogEntry> actualOut,
        Map<String, List<Integer>> strategyByMsgType,
        io.fixreplay.linker.LinkerConfig linkerConfig,
        CompareConfig compareConfig,
        String messageIdPrefix
    ) {
        FixComparator comparator = new FixComparator();
        Map<io.fixreplay.linker.LinkKey, List<FixLogEntry>> expectedIndex = buildIndex(expectedOut, strategyByMsgType, linkerConfig);

        Set<Long> consumedExpected = new HashSet<>();
        List<DiffReport.MessageResult> messageResults = new ArrayList<>();
        int unmatchedActual = 0;
        int ambiguous = 0;
        int matched = 0;

        for (FixLogEntry actual : actualOut) {
            String msgType = actual.message().msgType();
            List<Integer> tags = strategyByMsgType.get(msgType);
            if (tags == null || tags.isEmpty()) {
                unmatchedActual++;
                continue;
            }

            io.fixreplay.linker.LinkKey key = buildKey(msgType, tags, actual.message(), linkerConfig);
            if (key == null) {
                unmatchedActual++;
                continue;
            }

            List<FixLogEntry> candidates = expectedIndex.get(key);
            if (candidates == null || candidates.isEmpty()) {
                unmatchedActual++;
                continue;
            }
            if (candidates.size() > 1) {
                ambiguous++;
                continue;
            }

            FixLogEntry expected = candidates.get(0);
            if (!consumedExpected.add(expected.lineNumber())) {
                unmatchedActual++;
                continue;
            }
            expectedIndex.remove(key);

            CompareResult compareResult = comparator.compare(expected.message(), actual.message(), compareConfig);
            messageResults.add(new DiffReport.MessageResult(messageIdPrefix + expected.lineNumber() + "-" + actual.lineNumber(), compareResult));
            matched++;
        }

        int unmatchedExpected = Math.max(0, expectedOut.size() - consumedExpected.size());
        DiffReport diffReport = new DiffReport(messageResults);
        return new MatchResult(matched, unmatchedExpected, unmatchedActual, ambiguous, diffReport);
    }

    private static Map<io.fixreplay.linker.LinkKey, List<FixLogEntry>> buildIndex(
        List<FixLogEntry> messages,
        Map<String, List<Integer>> strategyByMsgType,
        io.fixreplay.linker.LinkerConfig linkerConfig
    ) {
        Map<io.fixreplay.linker.LinkKey, List<FixLogEntry>> index = new HashMap<>();
        for (FixLogEntry entry : messages) {
            String msgType = entry.message().msgType();
            List<Integer> tags = strategyByMsgType.get(msgType);
            if (tags == null || tags.isEmpty()) {
                continue;
            }
            io.fixreplay.linker.LinkKey key = buildKey(msgType, tags, entry.message(), linkerConfig);
            if (key == null) {
                continue;
            }
            index.computeIfAbsent(key, ignored -> new ArrayList<>()).add(entry);
        }
        return index;
    }

    private static io.fixreplay.linker.LinkKey buildKey(
        String msgType,
        List<Integer> tags,
        FixMessage message,
        io.fixreplay.linker.LinkerConfig linkerConfig
    ) {
        List<Integer> sortedTags = new ArrayList<>(tags);
        sortedTags.sort(Integer::compareTo);
        List<String> parts = new ArrayList<>(sortedTags.size());
        for (Integer tag : sortedTags) {
            String value = linkerConfig.normalizeValue(tag, message.get(tag));
            if (value == null || value.isEmpty()) {
                return null;
            }
            parts.add(value);
        }
        return new io.fixreplay.linker.LinkKey(msgType, List.copyOf(sortedTags), parts);
    }

    record MatchResult(int matched, int unmatchedExpected, int unmatchedActual, int ambiguous, DiffReport diffReport) {
    }
}
