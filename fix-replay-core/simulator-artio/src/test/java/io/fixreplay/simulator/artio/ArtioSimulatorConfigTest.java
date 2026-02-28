package io.fixreplay.simulator.artio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ArtioSimulatorConfigTest {
    @Test
    void loadsUnifiedScenarioAndAppliesSessionAndStorageDefaults(@TempDir Path tempDir) throws IOException {
        Path scenario = tempDir.resolve("scenario.yaml");
        Files.writeString(
            scenario,
            """
                cache_folder: cache-artifacts
                sessions:
                  entry:
                    sender_comp_id: ENTRY_RACOMPID
                  exit:
                    target_comp_id: EXIT_RACOMPID
                simulator:
                  provider: artio
                  enabled: true
                  entry:
                    listen_port: 7001
                  exit:
                    listen_port: 7002
                """
        );

        ArtioSimulatorConfig config = ArtioSimulatorConfig.load(scenario);

        assertTrue(config.enabled());
        assertEquals("artio", config.provider());
        assertEquals("FIX.4.2", config.beginString());

        assertEquals("0.0.0.0", config.entry().listenHost());
        assertEquals(7001, config.entry().listenPort());
        assertEquals("FIX_GATEWAY", config.entry().localCompId());
        assertEquals("ENTRY_RACOMPID", config.entry().remoteCompId());

        assertEquals("0.0.0.0", config.exit().listenHost());
        assertEquals(7002, config.exit().listenPort());
        assertEquals("FIX_GATEWAY", config.exit().localCompId());
        assertEquals("EXIT_RACOMPID", config.exit().remoteCompId());

        assertTrue(config.routing().enabledMsgTypes().contains("D"));
        assertTrue(config.routing().dropAdminMessages());
        assertEquals(50_000, config.routing().maxQueueDepth());

        assertFalse(config.mutation().enabled());
        assertEquals(ArtioSimulatorConfig.RuleSource.NONE, config.mutation().ruleSource());

        assertEquals(tempDir.resolve("cache-artifacts").toAbsolutePath().normalize(), config.storageDirs().workDir());
        assertEquals(config.storageDirs().workDir().resolve("aeron"), config.storageDirs().aeronDir());
        assertEquals(config.storageDirs().workDir().resolve("logs"), config.storageDirs().logDir());
    }

    @Test
    void allowsSinglePortTopologyWhenEnabled(@TempDir Path tempDir) throws IOException {
        Path scenario = tempDir.resolve("scenario.yaml");
        Files.writeString(
            scenario,
            """
                simulator:
                  enabled: true
                  entry:
                    listen_port: 7001
                  exit:
                    listen_port: 7001
                """
        );

        ArtioSimulatorConfig config = ArtioSimulatorConfig.load(scenario);
        assertEquals(7001, config.entry().listenPort());
        assertEquals(7001, config.exit().listenPort());
    }

    @Test
    void rejectsSinglePortTopologyWithDifferentHosts(@TempDir Path tempDir) throws IOException {
        Path scenario = tempDir.resolve("scenario.yaml");
        Files.writeString(
            scenario,
            """
                simulator:
                  enabled: true
                  entry:
                    listen_host: 0.0.0.0
                    listen_port: 7001
                  exit:
                    listen_host: 127.0.0.1
                    listen_port: 7001
                """
        );

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> ArtioSimulatorConfig.load(scenario));
        assertTrue(error.getMessage().contains("requires simulator.entry.listen_host and simulator.exit.listen_host"));
    }

    @Test
    void inlineRulesTakePrecedenceOverRulesFile(@TempDir Path tempDir) throws IOException {
        Path scenario = tempDir.resolve("scenario.yaml");
        Files.writeString(
            scenario,
            """
                simulator:
                  enabled: true
                  entry:
                    listen_port: 7101
                  exit:
                    listen_port: 7102
                  mutation:
                    enabled: true
                    strict_mode: true
                    rules_file: ./rules.yaml
                    rules_inline:
                      rules:
                        - name: add-prefix
                          when:
                            msgTypes: [D]
                          actions:
                            - type: prefix
                              tag: 11
                              value: RA-
                """
        );

        ArtioSimulatorConfig config = ArtioSimulatorConfig.load(scenario);

        assertTrue(config.mutation().enabled());
        assertTrue(config.mutation().strictMode());
        assertEquals(ArtioSimulatorConfig.RuleSource.INLINE, config.mutation().ruleSource());
        assertEquals(tempDir.resolve("rules.yaml").toAbsolutePath().normalize(), config.mutation().rulesFile());
        assertTrue(config.mutation().rulesInline().has("rules"));
    }

    @Test
    void missingPortsAllowedWhenSimulatorDisabled(@TempDir Path tempDir) throws IOException {
        Path scenario = tempDir.resolve("scenario.yaml");
        Files.writeString(
            scenario,
            """
                simulator:
                  enabled: false
                """
        );

        ArtioSimulatorConfig config = ArtioSimulatorConfig.load(scenario);

        assertFalse(config.enabled());
        assertEquals(0, config.entry().listenPort());
        assertEquals(0, config.exit().listenPort());
    }
}
