package io.fixreplay.runner;

import io.fixreplay.compare.DiffReport;
import io.fixreplay.linker.LinkReport;
import java.util.Objects;

public record OnlineRunResult(
    int sentCount,
    int receivedCount,
    int droppedCount,
    boolean timedOut,
    int matchedComparisons,
    int unmatchedExpected,
    int unmatchedActual,
    int ambiguous,
    DiffReport diffReport,
    LinkReport linkReport
) {
    public OnlineRunResult {
        if (
            sentCount < 0 ||
            receivedCount < 0 ||
            droppedCount < 0 ||
            matchedComparisons < 0 ||
            unmatchedExpected < 0 ||
            unmatchedActual < 0 ||
            ambiguous < 0
        ) {
            throw new IllegalArgumentException("Counts must be >= 0");
        }
        diffReport = Objects.requireNonNull(diffReport, "diffReport");
        linkReport = Objects.requireNonNull(linkReport, "linkReport");
    }

    public boolean passed() {
        return unmatchedExpected == 0 &&
            unmatchedActual == 0 &&
            ambiguous == 0 &&
            droppedCount == 0 &&
            diffReport.failedMessages() == 0;
    }
}
