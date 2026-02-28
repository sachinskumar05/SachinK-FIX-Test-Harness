package io.fixreplay.simulator.artio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ArtioSimulatorTest {
    @Test
    void startStopProvidesDiagnosticsAndCleansDirsWhenConfigured(@TempDir Path tempDir) throws IOException {
        int entryPort = freePort();
        int exitPort = freePort();
        Path scenario = tempDir.resolve("scenario.yaml");
        Files.writeString(
            scenario,
            """
                cache_folder: sim-cache
                simulator:
                  provider: artio
                  enabled: true
                  entry:
                    listen_port: %d
                  exit:
                    listen_port: %d
                  artio:
                    cleanup_on_stop: true
                """.formatted(entryPort, exitPort)
        );

        ArtioSimulatorConfig config = ArtioSimulatorConfig.load(scenario);
        Path workDir = config.storageDirs().workDir();
        Path aeronDir = config.storageDirs().aeronDir();
        Path logDir = config.storageDirs().logDir();

        try (ArtioSimulator simulator = ArtioSimulator.start(config)) {
            assertEquals(entryPort, simulator.entryPort());
            assertEquals(exitPort, simulator.exitPort());
            assertFalse(simulator.isReady());

            ArtioSimulator.Diagnostics diagnostics = simulator.diagnosticsSnapshot();
            assertEquals(0, diagnostics.sessionsAcquired());
            assertEquals(0, diagnostics.queueDepth());
            assertNull(diagnostics.lastError());
        }

        awaitDeleted(workDir);
        awaitDeleted(aeronDir);
        awaitDeleted(logDir);
    }

    @Test
    void simulatorRequiresArtioProvider(@TempDir Path tempDir) throws IOException {
        int entryPort = freePort();
        int exitPort = freePort();
        Path scenario = tempDir.resolve("scenario.yaml");
        Files.writeString(
            scenario,
            """
                simulator:
                  provider: quickfixj
                  enabled: true
                  entry:
                    listen_port: %d
                  exit:
                    listen_port: %d
                """.formatted(entryPort, exitPort)
        );

        ArtioSimulatorConfig config = ArtioSimulatorConfig.load(scenario);
        IllegalStateException expected = assertThrows(IllegalStateException.class, () -> ArtioSimulator.start(config));
        assertTrue(expected.getMessage().contains("Unsupported simulator provider"));
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static void awaitDeleted(Path path) {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(3));
        while (Files.exists(path) && Instant.now().isBefore(deadline)) {
            try {
                Thread.sleep(25);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        assertFalse(Files.exists(path));
    }
}
