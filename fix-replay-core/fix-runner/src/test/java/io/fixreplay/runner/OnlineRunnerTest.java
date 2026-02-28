package io.fixreplay.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.fixreplay.model.FixMessage;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class OnlineRunnerTest {
    @Test
    void runsOnlineFlowWithFakeTransport() {
        List<FixMessage> entry = List.of(
            FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=001|", '|'),
            FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-2|55=AAPL|10=002|", '|')
        );
        List<FixMessage> expectedExit = List.of(
            FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=011|", '|'),
            FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-2|55=AAPL|10=012|", '|')
        );
        List<FixMessage> received = List.of(
            FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=021|", '|'),
            FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-2|55=GOOG|10=022|", '|')
        );

        FakeTransport transport = new FakeTransport(received);
        OnlineRunnerConfig config = new OnlineRunnerConfig(
            io.fixreplay.linker.LinkerConfig.defaults(),
            io.fixreplay.compare.CompareConfig.defaults(),
            new io.fixreplay.model.MsgTypeFilter(),
            Duration.ofSeconds(1),
            16
        );

        OnlineRunResult result = new OnlineRunner().run(
            entry,
            expectedExit,
            transport,
            TransportSessionConfig.of(new SessionKey("ENTRY", "EXIT"), new SessionKey("EXIT", "ENTRY")),
            config
        );

        assertEquals(2, result.sentCount());
        assertEquals(2, result.receivedCount());
        assertEquals(0, result.droppedCount());
        assertFalse(result.timedOut());
        assertEquals(2, result.matchedComparisons());
        assertEquals(1, result.diffReport().failedMessages());
        assertTrue(transport.connected);
        assertTrue(transport.closed);
    }

    @Test
    void enforcesBoundedReceiveQueue() {
        List<FixMessage> entry = List.of(
            FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-1|10=001|", '|'),
            FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-2|10=002|", '|')
        );
        List<FixMessage> expectedExit = List.of(
            FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-1|10=011|", '|'),
            FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-2|10=012|", '|')
        );
        List<FixMessage> received = List.of(
            FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-1|10=021|", '|'),
            FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-2|10=022|", '|')
        );

        FakeTransport transport = new FakeTransport(received);
        OnlineRunnerConfig config = new OnlineRunnerConfig(
            io.fixreplay.linker.LinkerConfig.defaults(),
            io.fixreplay.compare.CompareConfig.defaults(),
            new io.fixreplay.model.MsgTypeFilter(),
            Duration.ofMillis(300),
            1
        );

        OnlineRunResult result = new OnlineRunner().run(
            entry,
            expectedExit,
            transport,
            TransportSessionConfig.of(new SessionKey("ENTRY", "EXIT"), new SessionKey("EXIT", "ENTRY")),
            config
        );

        assertEquals(1, result.droppedCount());
        assertTrue(result.unmatchedExpected() >= 1 || result.unmatchedActual() >= 1 || result.timedOut());
    }

    private static final class FakeTransport implements FixTransport {
        private final List<FixMessage> scriptedResponses;
        private final List<FixMessage> sent = new ArrayList<>();
        private Consumer<FixMessage> callback = ignored -> {};
        private int cursor;
        private boolean connected;
        private boolean closed;

        private FakeTransport(List<FixMessage> scriptedResponses) {
            this.scriptedResponses = scriptedResponses;
        }

        @Override
        public void connect(TransportSessionConfig sessionConfig) {
            this.connected = true;
        }

        @Override
        public void onReceive(Consumer<FixMessage> callback) {
            this.callback = callback;
        }

        @Override
        public void send(FixMessage message) {
            sent.add(message);
            if (cursor < scriptedResponses.size()) {
                callback.accept(scriptedResponses.get(cursor++));
            }
        }

        @Override
        public void close() {
            this.closed = true;
        }
    }
}
