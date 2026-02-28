package io.fixreplay.runner;

import io.fixreplay.compare.DiffReport;
import io.fixreplay.linker.DeterministicLinker;
import io.fixreplay.linker.LinkReport;
import io.fixreplay.loader.FixLogEntry;
import io.fixreplay.loader.FixLogScanner;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public final class OfflineRunner {
    private final FixLogScanner scanner;

    public OfflineRunner() {
        this(new FixLogScanner());
    }

    public OfflineRunner(FixLogScanner scanner) {
        this.scanner = scanner;
    }

    public OfflineRunResult run(ScenarioConfig config) throws IOException {
        Map<SessionKey, Path> inputFiles = config.resolveInputFiles();
        Map<SessionKey, Path> expectedFiles = config.resolveExpectedFiles();
        Map<SessionKey, Path> actualFiles = config.resolveActualFiles();
        boolean hasActual = config.hasActualFolder();

        DeterministicLinker linker = new DeterministicLinker(config.linkerConfig());
        Map<SessionKey, LinkReport> linkReports = new LinkedHashMap<>();
        List<DiffReport.MessageResult> allResults = new ArrayList<>();

        int matched = 0;
        int unmatchedExpected = 0;
        int unmatchedActual = 0;
        int ambiguous = 0;

        TreeSet<SessionKey> sessions = new TreeSet<>((left, right) -> left.id().compareTo(right.id()));
        sessions.addAll(expectedFiles.keySet());
        sessions.addAll(inputFiles.keySet());
        sessions.addAll(actualFiles.keySet());

        for (SessionKey session : sessions) {
            List<FixLogEntry> input = loadEntries(inputFiles.get(session), config);
            List<FixLogEntry> expected = loadEntries(expectedFiles.get(session), config);
            if (expected.isEmpty()) {
                continue;
            }

            LinkReport linkReport = linker.link(input, expected);
            linkReports.put(session, linkReport);

            if (!hasActual) {
                unmatchedExpected += linkReport.unmatched();
                ambiguous += linkReport.ambiguous();
                continue;
            }

            List<FixLogEntry> actual = loadEntries(actualFiles.get(session), config);
            MessageMatching.MatchResult matchResult = MessageMatching.matchAndCompare(
                expected,
                actual,
                linkReport.strategyByMsgType(),
                config.linkerConfig(),
                config.compareConfig(),
                session.id() + ":"
            );
            matched += matchResult.matched();
            unmatchedExpected += matchResult.unmatchedExpected();
            unmatchedActual += matchResult.unmatchedActual();
            ambiguous += matchResult.ambiguous();
            allResults.addAll(matchResult.diffReport().messages());
        }

        DiffReport diffReport = new DiffReport(allResults);
        return new OfflineRunResult(hasActual, matched, unmatchedExpected, unmatchedActual, ambiguous, diffReport, linkReports);
    }

    private List<FixLogEntry> loadEntries(Path path, ScenarioConfig config) {
        if (path == null) {
            return List.of();
        }
        AtomicLong lineNumber = new AtomicLong(1L);
        try (Stream<io.fixreplay.loader.FixRawMessage> messages = scanner.scan(path)) {
            return messages
                .map(io.fixreplay.loader.FixRawMessage::toFixMessage)
                .filter(message -> config.msgTypeFilter().accepts(message))
                .map(message -> new FixLogEntry(lineNumber.getAndIncrement(), message))
                .toList();
        }
    }
}
