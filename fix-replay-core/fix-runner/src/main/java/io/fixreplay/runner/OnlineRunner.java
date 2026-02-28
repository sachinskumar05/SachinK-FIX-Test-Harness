package io.fixreplay.runner;

import io.fixreplay.compare.DiffReport;
import io.fixreplay.linker.DeterministicLinker;
import io.fixreplay.linker.LinkReport;
import io.fixreplay.loader.FixLogEntry;
import io.fixreplay.model.FixMessage;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class OnlineRunner {
    public OnlineRunResult run(
        List<FixMessage> entryMessages,
        List<FixMessage> expectedExitMessages,
        FixTransport transport,
        TransportSessionConfig sessionConfig,
        OnlineRunnerConfig config
    ) {
        List<FixLogEntry> entry = asEntries(entryMessages, config.msgTypeFilter());
        List<FixLogEntry> expected = asEntries(expectedExitMessages, config.msgTypeFilter());

        DeterministicLinker linker = new DeterministicLinker(config.linkerConfig());
        LinkReport linkReport = linker.link(entry, expected);

        BlockingQueue<FixMessage> queue = new ArrayBlockingQueue<>(config.queueCapacity());
        AtomicInteger dropped = new AtomicInteger();

        transport.onReceive(message -> {
            if (!config.msgTypeFilter().accepts(message.msgType())) {
                return;
            }
            if (!queue.offer(message)) {
                dropped.incrementAndGet();
            }
        });

        int sent = 0;
        List<FixMessage> received = new ArrayList<>();
        boolean timedOut = false;
        Duration timeout = config.receiveTimeout();

        try {
            transport.connect(sessionConfig);
            for (FixMessage message : entryMessages) {
                if (!config.msgTypeFilter().accepts(message.msgType())) {
                    continue;
                }
                transport.send(message);
                sent++;
            }

            long deadlineNanos = System.nanoTime() + timeout.toNanos();
            while (received.size() < expected.size()) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    timedOut = true;
                    break;
                }
                FixMessage next = queue.poll(remainingNanos, TimeUnit.NANOSECONDS);
                if (next == null) {
                    timedOut = true;
                    break;
                }
                received.add(next);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            timedOut = true;
        } finally {
            transport.close();
        }

        List<FixLogEntry> actual = asEntries(received, config.msgTypeFilter());
        MessageMatching.MatchResult match = MessageMatching.matchAndCompare(
            expected,
            actual,
            linkReport.strategyByMsgType(),
            config.linkerConfig(),
            config.compareConfig(),
            "online:"
        );
        DiffReport diffReport = match.diffReport();

        return new OnlineRunResult(
            sent,
            received.size(),
            dropped.get(),
            timedOut,
            match.matched(),
            match.unmatchedExpected(),
            match.unmatchedActual(),
            match.ambiguous(),
            diffReport,
            linkReport
        );
    }

    private static List<FixLogEntry> asEntries(List<FixMessage> messages, io.fixreplay.model.MsgTypeFilter filter) {
        List<FixLogEntry> entries = new ArrayList<>();
        long lineNumber = 1L;
        for (FixMessage message : messages) {
            if (!filter.accepts(message.msgType())) {
                continue;
            }
            entries.add(new FixLogEntry(lineNumber++, message));
        }
        return List.copyOf(entries);
    }
}
