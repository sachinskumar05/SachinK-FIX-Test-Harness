package io.fixreplay.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class FixReplayRunnerTest {
    @Test
    void producesReplaySummary() {
        FixReplayRunner runner = new FixReplayRunner();

        ReplayResult result = runner.run(
            List.of("8=FIX.4.2|35=D|11=ABC123|55=MSFT|", "8=FIX.4.2|35=8|11=ABC123|"),
            List.of("8=FIX.4.2|35=D|11=ABC123|55=AAPL|", "8=FIX.4.2|35=8|11=ABC123|")
        );

        assertEquals(2, result.totalMessages());
        assertEquals(1, result.linkedPairs());
        assertEquals(1, result.diffCount());
    }
}
