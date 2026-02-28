package io.fixreplay.simulator.artio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ArtioMutationEngineTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void appliesConfiguredActionsDeterministically() throws Exception {
        JsonNode inlineRules = MAPPER.readTree(
            """
                {
                  "rules": [
                    {
                      "name": "prefix-clordid",
                      "when": { "msgTypes": ["D"], "conditions": [ { "tag": 11, "exists": true } ] },
                      "actions": [ { "type": "prefix", "tag": 11, "value": "RA-" } ]
                    },
                    {
                      "name": "set-custom",
                      "when": { "msgTypes": ["D"] },
                      "actions": [ { "type": "set", "tag": 9001, "value": "RAPID_ADDITION" } ]
                    },
                    {
                      "name": "regex-account",
                      "when": { "msgTypes": ["D"], "conditions": [ { "tag": 1, "exists": true } ] },
                      "actions": [
                        {
                          "type": "regex_replace",
                          "tag": 1,
                          "pattern": "^(.{0,4}).*$",
                          "replacement": "$1XXXX"
                        }
                      ]
                    },
                    {
                      "name": "remove-temp",
                      "actions": [ { "type": "remove", "tag": 9999 } ]
                    },
                    {
                      "name": "copy-to-origclordid",
                      "actions": [ { "type": "copy", "fromTag": 11, "toTag": 41 } ]
                    }
                  ]
                }
                """
        );

        ArtioSimulatorConfig.Mutation mutation = new ArtioSimulatorConfig.Mutation(
            true,
            false,
            inlineRules,
            null
        );
        ArtioMutationEngine engine = ArtioMutationEngine.fromConfig(mutation);

        Map<Integer, String> fields = new HashMap<>();
        fields.put(35, "D");
        fields.put(11, "ABC123");
        fields.put(1, "ACCOUNT12345");
        fields.put(9999, "DROP_ME");

        engine.apply("D", fields);

        assertEquals("RA-ABC123", fields.get(11));
        assertEquals("RA-ABC123", fields.get(41));
        assertEquals("ACCOXXXX", fields.get(1));
        assertEquals("RAPID_ADDITION", fields.get(9001));
        assertFalse(fields.containsKey(9999));
    }

    @Test
    void strictModeFailsOnMissingTagReference() throws Exception {
        JsonNode inlineRules = MAPPER.readTree(
            """
                {
                  "rules": [
                    {
                      "name": "prefix-required",
                      "when": { "msgTypes": ["D"] },
                      "actions": [ { "type": "prefix", "tag": 11, "value": "RA-" } ]
                    }
                  ]
                }
                """
        );

        ArtioSimulatorConfig.Mutation mutation = new ArtioSimulatorConfig.Mutation(
            true,
            true,
            inlineRules,
            null
        );
        ArtioMutationEngine engine = ArtioMutationEngine.fromConfig(mutation);

        Map<Integer, String> fields = new HashMap<>();
        fields.put(35, "D");

        IllegalStateException error = assertThrows(IllegalStateException.class, () -> engine.apply("D", fields));
        assertTrue(error.getMessage().contains("strict_mode"));
    }
}
