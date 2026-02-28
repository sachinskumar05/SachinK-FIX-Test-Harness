package io.fixreplay.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ScenarioConfigTest {
    @Test
    void loadsJsonScenarioConfig(@TempDir Path tempDir) throws IOException {
        Path input = Files.createDirectory(tempDir.resolve("input"));
        Path expected = Files.createDirectory(tempDir.resolve("expected"));
        Path configFile = tempDir.resolve("scenario.json");

        String json = """
            {
              "inputFolder": "input",
              "expectedFolder": "expected",
              "msgTypeFilter": ["D", "8"],
              "compare": {
                "excludeTimeLikeTags": true
              }
            }
            """;
        Files.writeString(configFile, json);

        ScenarioConfig config = ScenarioConfig.load(configFile);

        assertEquals(input, config.inputFolder());
        assertEquals(expected, config.expectedFolder());
        assertTrue(config.msgTypeFilter().accepts("D"));
        assertTrue(config.compareConfig().tagsToCompare("D", java.util.Set.of(60), java.util.Set.of(60)).isEmpty());
    }

    @Test
    void loadsYamlScenarioConfig(@TempDir Path tempDir) throws IOException {
        Path input = Files.createDirectory(tempDir.resolve("in-box"));
        Path expected = Files.createDirectory(tempDir.resolve("out-box"));
        Path configFile = tempDir.resolve("scenario.yaml");

        String yaml = """
            inputFolder: in-box
            expectedFolder: out-box
            linker:
              candidateTags: [11, 41]
              candidateCombinations:
                - [11]
                - [41]
            sessionMappingRules:
              - regex: "^(?<sender>[^_]+)_(?<target>[^.]+)\\\\.(?<side>in|out)$"
                senderGroup: sender
                targetGroup: target
                sideGroup: side
            """;
        Files.writeString(configFile, yaml);

        ScenarioConfig config = ScenarioConfig.load(configFile);

        assertEquals(input, config.inputFolder());
        assertEquals(expected, config.expectedFolder());
        assertEquals(2, config.linkerConfig().candidateTags().size());
        assertEquals(1, config.sessionMappingRules().size());
    }

    @Test
    void simulatorDefaultsToDisabledWhenSectionMissing(@TempDir Path tempDir) throws IOException {
        Path input = Files.createDirectory(tempDir.resolve("input"));
        Path expected = Files.createDirectory(tempDir.resolve("expected"));
        Path configFile = tempDir.resolve("scenario.yaml");

        String yaml = """
            inputFolder: input
            expectedFolder: expected
            """;
        Files.writeString(configFile, yaml);

        ScenarioConfig config = ScenarioConfig.load(configFile);

        assertFalse(config.simulator().enabled());
        assertEquals("none", config.simulator().provider());
    }

    @Test
    void parsesSimulatorSectionAndResolvesRelativeRulesPath(@TempDir Path tempDir) throws IOException {
        Path input = Files.createDirectory(tempDir.resolve("input"));
        Path expected = Files.createDirectory(tempDir.resolve("expected"));
        Path configFile = tempDir.resolve("scenario.yaml");

        String yaml = """
            inputFolder: input
            expectedFolder: expected
            cache_folder: cache
            sessions:
              entry:
                sender_comp_id: ENTRY_A
                target_comp_id: GATEWAY_A
              exit:
                sender_comp_id: GATEWAY_B
                target_comp_id: EXIT_B
            simulator:
              provider: artio
              enabled: true
              begin_string: FIX.4.2
              entry:
                listen_host: 127.0.0.1
                listen_port: 7101
              exit:
                listen_host: 127.0.0.1
                listen_port: 7102
              mutation:
                enabled: true
                strict_mode: true
                rules_file: ./rules.yaml
            """;
        Files.writeString(configFile, yaml);

        ScenarioConfig config = ScenarioConfig.load(configFile);

        assertTrue(config.simulator().enabled());
        assertEquals("artio", config.simulator().provider());
        assertEquals(7101, config.simulator().entry().listenPort());
        assertEquals(7102, config.simulator().exit().listenPort());
        assertEquals(tempDir.resolve("rules.yaml").toAbsolutePath().normalize(), config.simulator().mutation().rulesFile());
        assertEquals(tempDir.resolve("cache").toAbsolutePath().normalize(), config.cacheFolder());
        assertEquals("ENTRY_A", config.sessions().entry().senderCompId());
        assertEquals("EXIT_B", config.sessions().exit().targetCompId());
    }

    @Test
    void parsesSessionInitiatorHostAndPort(@TempDir Path tempDir) throws IOException {
        Files.createDirectory(tempDir.resolve("input"));
        Files.createDirectory(tempDir.resolve("expected"));
        Path configFile = tempDir.resolve("scenario.yaml");

        String yaml = """
            inputFolder: input
            expectedFolder: expected
            sessions:
              entry:
                sender_comp_id: ENTRY
                target_comp_id: QFIX
                host: 127.0.0.1
                port: 11001
              exit:
                sender_comp_id: EXIT
                target_comp_id: QFIX
                connect_host: localhost
                connect_port: 11002
            """;
        Files.writeString(configFile, yaml);

        ScenarioConfig config = ScenarioConfig.load(configFile);

        assertEquals("127.0.0.1", config.sessions().entry().host());
        assertEquals(11001, config.sessions().entry().port());
        assertEquals("localhost", config.sessions().exit().host());
        assertEquals(11002, config.sessions().exit().port());
    }
}
