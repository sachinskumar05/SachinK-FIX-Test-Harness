package io.fixreplay.runner;

import io.fixreplay.compare.FixMessageComparator;
import io.fixreplay.linker.DeterministicLinker;
import io.fixreplay.loader.FixLogScanner;
import io.fixreplay.model.FixMessage;
import java.util.List;
import java.util.Set;

public final class FixReplayRunner {
    private final FixLogScanner scanner;
    private final DeterministicLinker linker;
    private final FixMessageComparator comparator;

    public FixReplayRunner() {
        this(new FixLogScanner(), new DeterministicLinker(), new FixMessageComparator());
    }

    public FixReplayRunner(FixLogScanner scanner, DeterministicLinker linker, FixMessageComparator comparator) {
        this.scanner = scanner;
        this.linker = linker;
        this.comparator = comparator;
    }

    public ReplayResult run(List<String> expectedLines, List<String> actualLines) {
        var expectedEntries = scanner.extract(expectedLines, '|');
        var actualEntries = scanner.extract(actualLines, '|');
        var actualLinks = linker.link(actualEntries);

        int compareCount = Math.min(expectedEntries.size(), actualEntries.size());
        int diffCount = 0;
        for (int i = 0; i < compareCount; i++) {
            diffCount += comparator.compare(expectedEntries.get(i).message(), actualEntries.get(i).message(), Set.of(8, 9, 10)).size();
        }

        return new ReplayResult(expectedEntries.size(), actualLinks.size(), diffCount);
    }

    public int runOnline(OnlineFixAdapter adapter, List<FixMessage> outboundMessages) {
        for (FixMessage message : outboundMessages) {
            adapter.send(message);
        }
        return outboundMessages.size();
    }
}
