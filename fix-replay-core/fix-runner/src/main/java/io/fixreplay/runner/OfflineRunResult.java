package io.fixreplay.runner;

import io.fixreplay.compare.DiffReport;
import io.fixreplay.linker.LinkReport;
import java.util.Map;
import java.util.Objects;

public record OfflineRunResult(
    boolean usedActualMessages,
    int matchedComparisons,
    int unmatchedExpected,
    int unmatchedActual,
    int ambiguous,
    DiffReport diffReport,
    Map<SessionKey, LinkReport> linkReports
) {
    public OfflineRunResult {
        if (matchedComparisons < 0 || unmatchedExpected < 0 || unmatchedActual < 0 || ambiguous < 0) {
            throw new IllegalArgumentException("Counts must be >= 0");
        }
        diffReport = Objects.requireNonNull(diffReport, "diffReport");
        linkReports = Map.copyOf(Objects.requireNonNull(linkReports, "linkReports"));
    }

    public boolean passed() {
        return unmatchedExpected == 0 && unmatchedActual == 0 && ambiguous == 0 && diffReport.failedMessages() == 0;
    }
}
