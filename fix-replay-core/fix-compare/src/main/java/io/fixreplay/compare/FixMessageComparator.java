package io.fixreplay.compare;

import io.fixreplay.model.FixMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public final class FixMessageComparator {
    public List<FixDiff> compare(FixMessage expected, FixMessage actual, Set<Integer> ignoredTags) {
        Set<Integer> tags = new TreeSet<>();
        tags.addAll(expected.fields().keySet());
        tags.addAll(actual.fields().keySet());

        List<FixDiff> diffs = new ArrayList<>();
        for (Integer tag : tags) {
            if (ignoredTags.contains(tag)) {
                continue;
            }
            String expectedValue = expected.get(tag);
            String actualValue = actual.get(tag);
            if (!java.util.Objects.equals(expectedValue, actualValue)) {
                diffs.add(new FixDiff(tag, expectedValue, actualValue));
            }
        }
        return List.copyOf(diffs);
    }
}
