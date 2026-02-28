package io.fixreplay.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OfflineRunnerTest {
    @Test
    void comparesExpectedAndActualFromFolders(@TempDir Path tempDir) throws IOException {
        Path input = Files.createDirectory(tempDir.resolve("input"));
        Path expected = Files.createDirectory(tempDir.resolve("expected"));
        Path actual = Files.createDirectory(tempDir.resolve("actual"));

        Files.writeString(
            input.resolve("BUY_SELL.in"),
            String.join(
                "\n",
                "8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=001|",
                "8=FIX.4.4|35=D|11=ORD-2|55=AAPL|10=002|"
            ) + "\n"
        );
        Files.writeString(
            expected.resolve("BUY_SELL.out"),
            String.join(
                "\n",
                "8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=011|",
                "8=FIX.4.4|35=D|11=ORD-2|55=AAPL|10=012|"
            ) + "\n"
        );
        Files.writeString(
            actual.resolve("BUY_SELL.out"),
            String.join(
                "\n",
                "8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=021|",
                "8=FIX.4.4|35=D|11=ORD-2|55=GOOG|10=022|"
            ) + "\n"
        );

        ScenarioConfig scenario = ScenarioConfig.builder()
            .inputFolder(input)
            .expectedFolder(expected)
            .actualFolder(actual)
            .build();

        OfflineRunResult result = new OfflineRunner().run(scenario);

        assertEquals(2, result.matchedComparisons());
        assertEquals(0, result.unmatchedExpected());
        assertEquals(0, result.unmatchedActual());
        assertEquals(0, result.ambiguous());
        assertEquals(1, result.diffReport().failedMessages());
        assertTrue(result.linkReports().containsKey(new SessionKey("BUY", "SELL")));
    }

    @Test
    void validatesExpectedConsistencyWhenActualFolderMissing(@TempDir Path tempDir) throws IOException {
        Path input = Files.createDirectory(tempDir.resolve("input"));
        Path expected = Files.createDirectory(tempDir.resolve("expected"));

        Files.writeString(
            input.resolve("S1_T1.in"),
            "8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=001|\n"
        );
        Files.writeString(
            expected.resolve("S1_T1.out"),
            "8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=011|\n"
        );

        ScenarioConfig scenario = ScenarioConfig.builder()
            .inputFolder(input)
            .expectedFolder(expected)
            .build();

        OfflineRunResult result = new OfflineRunner().run(scenario);

        assertFalse(result.usedActualMessages());
        assertEquals(0, result.unmatchedExpected());
        assertEquals(0, result.ambiguous());
        assertEquals(0, result.diffReport().failedMessages());
    }
}
