package uk.fixreplay.simulator.artio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ArtioSimulatorAppTest {
    @Test
    void returnsConfigErrorWhenScenarioArgumentMissing() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int exitCode = ArtioSimulatorApp.run(new String[0], new PrintStream(out), new PrintStream(err));

        assertEquals(3, exitCode);
        assertTrue(err.toString().contains("Missing required argument"));
    }

    @Test
    void returnsConfigErrorWhenSimulatorDisabled(@TempDir Path tempDir) throws IOException {
        Path scenario = tempDir.resolve("scenario.yaml");
        Files.writeString(
            scenario,
            """
                simulator:
                  provider: artio
                  enabled: false
                """
        );

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int exitCode = ArtioSimulatorApp.run(
            new String[] {"--scenario", scenario.toString()},
            new PrintStream(out),
            new PrintStream(err)
        );

        assertEquals(3, exitCode);
        assertTrue(err.toString().contains("simulator.enabled must be true"));
    }

    @Test
    void returnsConfigErrorWhenProviderIsNotArtio(@TempDir Path tempDir) throws IOException {
        Path scenario = tempDir.resolve("scenario.yaml");
        Files.writeString(
            scenario,
            """
                simulator:
                  provider: quickfixj
                  enabled: true
                  entry:
                    listen_port: 7001
                  exit:
                    listen_port: 7002
                """
        );

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int exitCode = ArtioSimulatorApp.run(
            new String[] {"--scenario", scenario.toString()},
            new PrintStream(out),
            new PrintStream(err)
        );

        assertEquals(3, exitCode);
        assertTrue(err.toString().contains("simulator.provider must be 'artio'"));
    }
}
