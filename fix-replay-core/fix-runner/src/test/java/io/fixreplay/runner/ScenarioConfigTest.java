package io.fixreplay.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
