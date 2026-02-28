package io.fixreplay.compare;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class CompareResult {
    private final String msgType;
    private final List<Integer> missingTags;
    private final List<Integer> extraTags;
    private final Map<Integer, ValueDifference> differingValues;

    public CompareResult(
        String msgType,
        List<Integer> missingTags,
        List<Integer> extraTags,
        Map<Integer, ValueDifference> differingValues
    ) {
        this.msgType = msgType;
        this.missingTags = List.copyOf(Objects.requireNonNull(missingTags, "missingTags"));
        this.extraTags = List.copyOf(Objects.requireNonNull(extraTags, "extraTags"));
        this.differingValues = Map.copyOf(Objects.requireNonNull(differingValues, "differingValues"));
    }

    public String msgType() {
        return msgType;
    }

    public boolean passed() {
        return missingTags.isEmpty() && extraTags.isEmpty() && differingValues.isEmpty();
    }

    public List<Integer> missingTags() {
        return missingTags;
    }

    public List<Integer> extraTags() {
        return extraTags;
    }

    public Map<Integer, ValueDifference> differingValues() {
        return differingValues;
    }

    public int differenceCount() {
        return missingTags.size() + extraTags.size() + differingValues.size();
    }

    public record ValueDifference(String expected, String actual) {
    }
}
