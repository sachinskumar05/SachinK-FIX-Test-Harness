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
                    target_comp_id: FIX_GATEWAY
                  exit:
                    sender_comp_id: FIX_GATEWAY
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
        assertTrue(config.storageDirs().deleteOnStart());
        assertFalse(config.storageDirs().deleteOnStop());
    }

    @Test
    void defaultsSimulatorCompIdsFromSessionsSection(@TempDir Path tempDir) throws IOException {
        Path scenario = tempDir.resolve("scenario.yaml");
        Files.writeString(
            scenario,
            """
                sessions:
                  entry:
                    sender_comp_id: ENTRY_CUSTOM
                    target_comp_id: GATEWAY_ENTRY
                  exit:
                    sender_comp_id: GATEWAY_EXIT
                    target_comp_id: EXIT_CUSTOM
                simulator:
                  provider: artio
                  enabled: true
                  entry:
                    listen_port: 7401
                  exit:
                    listen_port: 7402
                io:
                  mode: online
                """
        );

        ArtioSimulatorConfig config = ArtioSimulatorConfig.load(scenario);

        assertEquals("GATEWAY_ENTRY", config.entry().localCompId());
        assertEquals("ENTRY_CUSTOM", config.entry().remoteCompId());
        assertEquals("GATEWAY_EXIT", config.exit().localCompId());
        assertEquals("EXIT_CUSTOM", config.exit().remoteCompId());
    }

    @Test
    void derivesMissingSimulatorCompIdsFromSessionsPerspective(@TempDir Path tempDir) throws IOException {
        Path scenario = tempDir.resolve("scenario.yaml");
        Files.writeString(
            scenario,
            """
                sessions:
                  entry:
                    sender_comp_id: ENTRY_RACOMPID
                    target_comp_id: FIX_GATEWAY
                  exit:
                    sender_comp_id: FIX_GATEWAY
                    target_comp_id: EXIT_RACOMPID
                simulator:
                  provider: artio
                  enabled: true
                  entry:
                    listen_port: 7401
                    local_comp_id: ENTRY_LOCAL_OVERRIDE
                  exit:
                    listen_port: 7402
                    remote_comp_id: EXIT_REMOTE_OVERRIDE
                """
        );

        ArtioSimulatorConfig config = ArtioSimulatorConfig.load(scenario);

        assertEquals("ENTRY_LOCAL_OVERRIDE", config.entry().localCompId());
        assertEquals("ENTRY_RACOMPID", config.entry().remoteCompId());
        assertEquals("FIX_GATEWAY", config.exit().localCompId());
        assertEquals("EXIT_REMOTE_OVERRIDE", config.exit().remoteCompId());
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
    void rejectsEnabledSimulatorWhenProviderIsNotArtio(@TempDir Path tempDir) throws IOException {
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

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> ArtioSimulatorConfig.load(scenario));
        assertTrue(error.getMessage().contains("simulator.provider must be 'artio'"));
    }

    @Test
    void rejectsEnabledSimulatorWhenPortsMissing(@TempDir Path tempDir) throws IOException {
        Path scenario = tempDir.resolve("scenario.yaml");
        Files.writeString(
            scenario,
            """
                simulator:
                  provider: artio
                  enabled: true
                  entry:
                    listen_port: 7001
                """
        );

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> ArtioSimulatorConfig.load(scenario));
        assertTrue(error.getMessage().contains("simulator.exit.listen_port"));
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
    void emptyInlineRulesFallBackToRulesFile(@TempDir Path tempDir) throws IOException {
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
                    strict_mode: false
                    rules_file: ./rules.yaml
                    rules_inline:
                      rules: []
                """
        );

        ArtioSimulatorConfig config = ArtioSimulatorConfig.load(scenario);

        assertTrue(config.mutation().enabled());
        assertFalse(config.mutation().strictMode());
        assertEquals(ArtioSimulatorConfig.RuleSource.FILE, config.mutation().ruleSource());
        assertEquals(tempDir.resolve("rules.yaml").toAbsolutePath().normalize(), config.mutation().rulesFile());
    }

    @Test
    void parsesArtioWorkDirAndDeleteFlags(@TempDir Path tempDir) throws IOException {
        Path scenario = tempDir.resolve("scenario.yaml");
        Files.writeString(
            scenario,
            """
                simulator:
                  provider: artio
                  enabled: true
                  entry:
                    listen_port: 7301
                  exit:
                    listen_port: 7302
                  artio:
                    work_dir: ./out/sim/artio
                    delete_on_start: true
                    delete_on_stop: true
                """
        );

        ArtioSimulatorConfig config = ArtioSimulatorConfig.load(scenario);

        assertEquals(tempDir.resolve("out/sim/artio").toAbsolutePath().normalize(), config.storageDirs().workDir());
        assertEquals(config.storageDirs().workDir().resolve("aeron"), config.storageDirs().aeronDir());
        assertEquals(config.storageDirs().workDir().resolve("logs"), config.storageDirs().logDir());
        assertTrue(config.storageDirs().deleteOnStart());
        assertTrue(config.storageDirs().deleteOnStop());
    }

    @Test
    void derivesNonEmptyCompIdsWhenBlankValuesProvided(@TempDir Path tempDir) throws IOException {
        Path scenario = tempDir.resolve("scenario.yaml");
        Files.writeString(
            scenario,
            """
                sessions:
                  entry:
                    sender_comp_id: " "
                    target_comp_id: " "
                  exit:
                    sender_comp_id: " "
                    target_comp_id: " "
                simulator:
                  provider: artio
                  enabled: true
                  entry:
                    listen_port: 7301
                    local_comp_id: " "
                    remote_comp_id: " "
                  exit:
                    listen_port: 7302
                    local_comp_id: " "
                    remote_comp_id: " "
                """
        );

        ArtioSimulatorConfig config = ArtioSimulatorConfig.load(scenario);
        assertEquals("FIX_GATEWAY", config.entry().localCompId());
        assertEquals("ENTRY_RACOMPID", config.entry().remoteCompId());
        assertEquals("FIX_GATEWAY", config.exit().localCompId());
        assertEquals("EXIT_RACOMPID", config.exit().remoteCompId());
    }

    @Test
    void supportsTopLevelMutationRulesFromUnifiedScenario(@TempDir Path tempDir) throws IOException {
        Path scenario = tempDir.resolve("scenario.yaml");
        Files.writeString(
            scenario,
            """
                simulator:
                  enabled: true
                  entry:
                    listen_port: 7201
                  exit:
                    listen_port: 7202
                mutation:
                  enabled: true
                  strict_mode: false
                  rules_inline:
                    rules:
                      - name: RA_prefix_ClOrdID
                        when:
                          msgTypes: [D, G, F]
                          conditions:
                            - tag: 11
                              exists: true
                        actions:
                          - type: prefix
                            tag: 11
                            value: RA-
                      - name: RA_set_custom_tag
                        when:
                          msgTypes: [D]
                        actions:
                          - type: set
                            tag: 9001
                            value: RAPID_ADDITION
                """
        );

        ArtioSimulatorConfig config = ArtioSimulatorConfig.load(scenario);

        assertTrue(config.mutation().enabled());
        assertFalse(config.mutation().strictMode());
        assertEquals(ArtioSimulatorConfig.RuleSource.INLINE, config.mutation().ruleSource());
        assertTrue(config.mutation().rulesInline().has("rules"));
        assertEquals("RA_prefix_ClOrdID", config.mutation().rulesInline().path("rules").get(0).path("name").asText());
        assertEquals("prefix", config.mutation().rulesInline().path("rules").get(0).path("actions").get(0).path("type").asText());
    }

    @Test
    void missingPortsAllowedWhenSimulatorDisabled(@TempDir Path tempDir) throws IOException {
        Path scenario = tempDir.resolve("scenario.yaml");
        Files.writeString(
            scenario,
            """
                simulator:
                  provider: none
                  enabled: false
                """
        );

        ArtioSimulatorConfig config = ArtioSimulatorConfig.load(scenario);

        assertFalse(config.enabled());
        assertEquals("none", config.provider());
        assertTrue(config.providerSupportedByArtioModule());
        assertEquals(0, config.entry().listenPort());
        assertEquals(0, config.exit().listenPort());
    }
}
