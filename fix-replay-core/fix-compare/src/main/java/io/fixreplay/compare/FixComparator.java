package io.fixreplay.compare;

import io.fixreplay.model.FixMessage;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public final class FixComparator {
    public CompareResult compare(FixMessage expected, FixMessage actual, CompareConfig config) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(actual, "actual");
        Objects.requireNonNull(config, "config");

        String msgType = expected.msgType() != null ? expected.msgType() : actual.msgType();
        Set<Integer> tagsToCompare = config.tagsToCompare(msgType, expected.fields().keySet(), actual.fields().keySet());
        TreeSet<Integer> orderedTags = new TreeSet<>(tagsToCompare);

        List<Integer> missingTags = new ArrayList<>();
        List<Integer> extraTags = new ArrayList<>();
        Map<Integer, CompareResult.ValueDifference> differing = new LinkedHashMap<>();

        for (Integer tag : orderedTags) {
            String expectedRaw = expected.get(tag);
            String actualRaw = actual.get(tag);

            if (expectedRaw != null && actualRaw == null) {
                missingTags.add(tag);
                continue;
            }
            if (expectedRaw == null && actualRaw != null) {
                extraTags.add(tag);
                continue;
            }
            if (expectedRaw == null) {
                continue;
            }

            String expectedValue = config.normalizeValue(tag, expectedRaw);
            String actualValue = config.normalizeValue(tag, actualRaw);
            if (!Objects.equals(expectedValue, actualValue)) {
                differing.put(tag, new CompareResult.ValueDifference(expectedValue, actualValue));
            }
        }

        return new CompareResult(msgType, missingTags, extraTags, differing);
    }
}
