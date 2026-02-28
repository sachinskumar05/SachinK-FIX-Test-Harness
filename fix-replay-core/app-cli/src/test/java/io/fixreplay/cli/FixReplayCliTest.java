package io.fixreplay.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class FixReplayCliTest {
    @Test
    void executesDefaultCommand() {
        int exitCode = new CommandLine(new FixReplayCli()).execute();

        assertEquals(0, exitCode);
        assertTrue(FixReplayCli.currentVersion().contains("SNAPSHOT") || FixReplayCli.currentVersion().matches("\\d+\\.\\d+.*"));
    }
}
