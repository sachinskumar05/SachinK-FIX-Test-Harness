package io.fixreplay.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fixreplay.model.FixMessage;
import io.fixreplay.runner.FixTransport;
import io.fixreplay.runner.ScenarioConfig;
import io.fixreplay.runner.TransportSessionConfig;
import io.fixreplay.simulator.artio.ArtioSimulatorConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FixReplayCliTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void executesDefaultCommand() {
        int exitCode = FixReplayCli.newCommandLine().execute();

        assertEquals(0, exitCode);
        assertTrue(FixReplayCli.currentVersion().contains("SNAPSHOT") || FixReplayCli.currentVersion().matches("\\d+\\.\\d+.*"));
    }

    @Test
    void scanCommandWritesSummaryJson(@TempDir Path tempDir) throws IOException {
        Path logFile = tempDir.resolve("mixed.log");
        Files.writeString(
            logFile,
            String.join(
                "\n",
                "2026-01-10 10:15:30.100 INFO IN [8=FIX.4.4^A35=D^A49=BUY^A56=SELL^A11=ORD-1^A10=001^A]",
                "noise line",
                "2026-01-10 10:15:31.100 INFO OUT 8=FIX.4.4|35=8|49=SELL|56=BUY|37=EX-1|10=002|"
            ) + "\n"
        );
        Path out = tempDir.resolve("scan.json");

        int exitCode = FixReplayCli.newCommandLine().execute(
            "scan",
            "--path",
            logFile.toString(),
            "--out",
            out.toString()
        );

        assertEquals(0, exitCode);
        JsonNode json = JSON.readTree(Files.readString(out));
        assertEquals(2, json.path("messageCount").asInt());
        assertEquals(1, json.path("msgTypeDistribution").path("D").asInt());
        assertEquals(1, json.path("msgTypeDistribution").path("8").asInt());
        assertEquals(2, json.path("sessionsDetected").size());
    }

    @Test
    void prepareCommandWritesLinkReportsToCache(@TempDir Path tempDir) throws IOException {
        Path input = Files.createDirectory(tempDir.resolve("input"));
        Path expected = Files.createDirectory(tempDir.resolve("expected"));
        Path cache = tempDir.resolve("cache");
        Path scenario = tempDir.resolve("scenario.yaml");

        Files.writeString(input.resolve("BUY_SELL.in"), "8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=001|\n");
        Files.writeString(expected.resolve("BUY_SELL.out"), "8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=011|\n");
        Files.writeString(
            scenario,
            String.join(
                "\n",
                "inputFolder: placeholder-in",
                "expectedFolder: placeholder-out",
                "msgTypeFilter: [D,8]"
            ) + "\n"
        );

        int exitCode = FixReplayCli.newCommandLine().execute(
            "prepare",
            "--in",
            input.toString(),
            "--expected",
            expected.toString(),
            "--scenario",
            scenario.toString(),
            "--cache",
            cache.toString()
        );

        assertEquals(0, exitCode);
        Path aggregated = cache.resolve("link-report.json");
        Path perSession = cache.resolve("link-report-BUY_SELL.json");
        assertTrue(Files.exists(aggregated));
        assertTrue(Files.exists(perSession));

        JsonNode json = JSON.readTree(Files.readString(aggregated));
        assertEquals(1, json.path("counts").path("sessions").asInt());
        assertTrue(json.path("linkReports").has("BUY_SELL"));
    }

    @Test
    void runOfflineReturnsCompareFailureExitAndWritesArtifacts(@TempDir Path tempDir) throws IOException {
        Path input = Files.createDirectory(tempDir.resolve("input"));
        Path expected = Files.createDirectory(tempDir.resolve("expected"));
        Path actual = Files.createDirectory(tempDir.resolve("actual"));
        Path scenario = tempDir.resolve("scenario.yaml");
        Path out = tempDir.resolve("offline.json");
        Path junit = tempDir.resolve("offline-junit.xml");

        Files.writeString(input.resolve("BUY_SELL.in"), "8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=001|\n");
        Files.writeString(expected.resolve("BUY_SELL.out"), "8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=011|\n");
        Files.writeString(actual.resolve("BUY_SELL.out"), "8=FIX.4.4|35=D|11=ORD-1|55=AAPL|10=021|\n");
        Files.writeString(
            scenario,
            String.join(
                "\n",
                "inputFolder: input",
                "expectedFolder: expected",
                "actualFolder: actual",
                "msgTypeFilter: [D,8]"
            ) + "\n"
        );

        int exitCode = FixReplayCli.newCommandLine().execute(
            "run-offline",
            "--scenario",
            scenario.toString(),
            "--out",
            out.toString(),
            "--junit",
            junit.toString()
        );

        assertEquals(2, exitCode);
        assertTrue(Files.exists(out));
        assertTrue(Files.exists(junit));
        JsonNode json = JSON.readTree(Files.readString(out));
        assertTrue(json.path("diffReport").path("failedMessages").asInt() >= 1);
    }

    @Test
    void runOfflineReturnsConfigErrorForMissingScenario(@TempDir Path tempDir) {
        int exitCode = FixReplayCli.newCommandLine().execute(
            "run-offline",
            "--scenario",
            tempDir.resolve("missing.yaml").toString()
        );

        assertEquals(3, exitCode);
    }

    @Test
    void runOnlineExecutesWithProvidedTransportClass(@TempDir Path tempDir) throws IOException {
        Path input = Files.createDirectory(tempDir.resolve("input"));
        Path expected = Files.createDirectory(tempDir.resolve("expected"));
        Path scenario = tempDir.resolve("scenario.yaml");
        Path out = tempDir.resolve("online.json");
        Path junit = tempDir.resolve("online-junit.xml");

        Files.writeString(input.resolve("BUY_SELL.in"), "8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=001|\n");
        Files.writeString(expected.resolve("BUY_SELL.out"), "8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=011|\n");
        Files.writeString(
            scenario,
            String.join(
                "\n",
                "inputFolder: input",
                "expectedFolder: expected",
                "msgTypeFilter: [D,8]"
            ) + "\n"
        );

        ScriptedTransport.reset(List.of(FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=099|", '|')));

        int exitCode = FixReplayCli.newCommandLine().execute(
            "run-online",
            "--scenario",
            scenario.toString(),
            "--transport-class",
            ScriptedTransport.class.getName(),
            "--transport-prop",
            "custom=value",
            "--receive-timeout-ms",
            "500",
            "--queue-capacity",
            "8",
            "--out",
            out.toString(),
            "--junit",
            junit.toString()
        );

        assertEquals(0, exitCode);
        assertTrue(Files.exists(out));
        assertTrue(Files.exists(junit));
        assertEquals(1, ScriptedTransport.connectCalls.get());
        assertEquals("value", ScriptedTransport.lastProperties.get("custom"));
    }

    @Test
    void runOnlineUsesInitiatorHostAndPortFromScenarioWhenNotProvided(@TempDir Path tempDir) throws IOException {
        Path input = Files.createDirectory(tempDir.resolve("input"));
        Path expected = Files.createDirectory(tempDir.resolve("expected"));
        Path scenario = tempDir.resolve("scenario.yaml");

        Files.writeString(input.resolve("BUY_SELL.in"), "8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=001|\n");
        Files.writeString(expected.resolve("BUY_SELL.out"), "8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=011|\n");
        Files.writeString(
            scenario,
            String.join(
                "\n",
                "inputFolder: input",
                "expectedFolder: expected",
                "sessions:",
                "  entry:",
                "    sender_comp_id: BUY",
                "    target_comp_id: SELL",
                "    host: 127.0.0.1",
                "    port: 11001",
                "  exit:",
                "    sender_comp_id: SELL",
                "    target_comp_id: BUY",
                "    host: 127.0.0.1",
                "    port: 11002"
            ) + "\n"
        );

        ScriptedTransport.reset(List.of(FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=099|", '|')));

        int exitCode = FixReplayCli.newCommandLine().execute(
            "run-online",
            "--scenario",
            scenario.toString(),
            "--transport-class",
            ScriptedTransport.class.getName(),
            "--receive-timeout-ms",
            "500",
            "--queue-capacity",
            "8"
        );

        assertEquals(0, exitCode);
        assertEquals("127.0.0.1", ScriptedTransport.lastProperties.get("artio.host"));
        assertEquals("11001", ScriptedTransport.lastProperties.get("artio.port"));
        assertEquals("127.0.0.1", ScriptedTransport.lastProperties.get("artio.exitHost"));
        assertEquals("11002", ScriptedTransport.lastProperties.get("artio.exitPort"));
    }

    @Test
    void runOnlineWithStartSimulatorSkipsWhenSimulatorDisabled(@TempDir Path tempDir) throws IOException {
        Path input = Files.createDirectory(tempDir.resolve("input"));
        Path expected = Files.createDirectory(tempDir.resolve("expected"));
        Path scenario = tempDir.resolve("scenario.yaml");
        Path out = tempDir.resolve("online-start-sim-disabled.json");
        Path junit = tempDir.resolve("online-start-sim-disabled-junit.xml");

        Files.writeString(input.resolve("BUY_SELL.in"), "8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=001|\n");
        Files.writeString(expected.resolve("BUY_SELL.out"), "8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=011|\n");
        Files.writeString(
            scenario,
            String.join(
                "\n",
                "inputFolder: input",
                "expectedFolder: expected",
                "msgTypeFilter: [D,8]",
                "simulator:",
                "  provider: artio",
                "  enabled: false"
            ) + "\n"
        );

        ScriptedTransport.reset(List.of(FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=099|", '|')));

        int exitCode = FixReplayCli.newCommandLine().execute(
            "run-online",
            "--scenario",
            scenario.toString(),
            "--transport-class",
            ScriptedTransport.class.getName(),
            "--start-simulator",
            "--out",
            out.toString(),
            "--junit",
            junit.toString()
        );

        assertEquals(0, exitCode);
        assertTrue(Files.exists(out));
        assertTrue(Files.exists(junit));
        assertEquals(1, ScriptedTransport.connectCalls.get());
    }

    @Test
    void applySimulatorTransportDefaultsUsesScenarioPortsAndWildcardHost(@TempDir Path tempDir) throws IOException {
        Files.createDirectory(tempDir.resolve("input"));
        Files.createDirectory(tempDir.resolve("expected"));
        Path scenario = tempDir.resolve("scenario.yaml");
        Files.writeString(
            scenario,
            String.join(
                "\n",
                "inputFolder: input",
                "expectedFolder: expected",
                "sessions:",
                "  entry:",
                "    sender_comp_id: ENTRY_RACOMPID",
                "    target_comp_id: FIX_GATEWAY",
                "  exit:",
                "    sender_comp_id: FIX_GATEWAY",
                "    target_comp_id: EXIT_RACOMPID",
                "simulator:",
                "  provider: artio",
                "  enabled: true",
                "  entry:",
                "    listen_host: 0.0.0.0",
                "    listen_port: 7101",
                "  exit:",
                "    listen_host: 0.0.0.0",
                "    listen_port: 7102"
            ) + "\n"
        );

        ScenarioConfig parsedScenario = ScenarioConfig.load(scenario);
        ArtioSimulatorConfig simulatorConfig = ArtioSimulatorConfig.fromScenario(parsedScenario);

        Map<String, String> defaults = FixReplayCli.applySimulatorTransportDefaults(Map.of("custom", "v"), simulatorConfig);
        assertEquals("127.0.0.1", defaults.get("artio.host"));
        assertEquals("7101", defaults.get("artio.port"));
        assertEquals("127.0.0.1", defaults.get("artio.exitHost"));
        assertEquals("7102", defaults.get("artio.exitPort"));
        assertEquals("v", defaults.get("custom"));

        Map<String, String> overridden = FixReplayCli.applySimulatorTransportDefaults(
            Map.of("artio.host", "example.com", "artio.port", "7999"),
            simulatorConfig
        );
        assertEquals("example.com", overridden.get("artio.host"));
        assertEquals("7999", overridden.get("artio.port"));
    }

    @Test
    void applyScenarioInitiatorTransportDefaultsUsesSessionEndpoints() {
        ScenarioConfig scenario = ScenarioConfig.builder()
            .sessions(
                new ScenarioConfig.Sessions(
                    new ScenarioConfig.SessionIdentity("ENTRY", "QFIX", "127.0.0.1", 7101),
                    new ScenarioConfig.SessionIdentity("EXIT", "QFIX", "127.0.0.1", 7102)
                )
            )
            .build();

        Map<String, String> defaults = FixReplayCli.applyScenarioInitiatorTransportDefaults(scenario, Map.of("custom", "v"));
        assertEquals("127.0.0.1", defaults.get("artio.host"));
        assertEquals("7101", defaults.get("artio.port"));
        assertEquals("127.0.0.1", defaults.get("artio.exitHost"));
        assertEquals("7102", defaults.get("artio.exitPort"));
        assertEquals("v", defaults.get("custom"));

        Map<String, String> overridden = FixReplayCli.applyScenarioInitiatorTransportDefaults(
            scenario,
            Map.of("artio.host", "example.com", "artio.port", "9999")
        );
        assertEquals("example.com", overridden.get("artio.host"));
        assertEquals("9999", overridden.get("artio.port"));
    }

    @Test
    void applyScenarioInitiatorTransportDefaultsIgnoresBlankHostAndNonPositivePort() {
        ScenarioConfig scenario = ScenarioConfig.builder()
            .sessions(
                new ScenarioConfig.Sessions(
                    new ScenarioConfig.SessionIdentity("ENTRY", "QFIX", " ", 0),
                    new ScenarioConfig.SessionIdentity("EXIT", "QFIX", null, -1)
                )
            )
            .build();

        Map<String, String> defaults = FixReplayCli.applyScenarioInitiatorTransportDefaults(scenario, Map.of("custom", "v"));

        assertEquals("v", defaults.get("custom"));
        assertTrue(!defaults.containsKey("artio.host"));
        assertTrue(!defaults.containsKey("artio.port"));
        assertTrue(!defaults.containsKey("artio.exitHost"));
        assertTrue(!defaults.containsKey("artio.exitPort"));
    }

    @Test
    void applySimulatorTransportDefaultsDoesNotOverrideScenarioDerivedTransportKeys(@TempDir Path tempDir) throws IOException {
        Files.createDirectory(tempDir.resolve("input"));
        Files.createDirectory(tempDir.resolve("expected"));
        Path scenario = tempDir.resolve("scenario.yaml");
        Files.writeString(
            scenario,
            String.join(
                "\n",
                "inputFolder: input",
                "expectedFolder: expected",
                "simulator:",
                "  provider: artio",
                "  enabled: true",
                "  entry:",
                "    listen_host: 0.0.0.0",
                "    listen_port: 7101",
                "  exit:",
                "    listen_host: 0.0.0.0",
                "    listen_port: 7102"
            ) + "\n"
        );

        ArtioSimulatorConfig simulatorConfig = ArtioSimulatorConfig.fromScenario(ScenarioConfig.load(scenario));
        Map<String, String> merged = FixReplayCli.applySimulatorTransportDefaults(
            Map.of("artio.host", "192.168.10.5", "artio.port", "62000"),
            simulatorConfig
        );

        assertEquals("192.168.10.5", merged.get("artio.host"));
        assertEquals("62000", merged.get("artio.port"));
        assertEquals("127.0.0.1", merged.get("artio.exitHost"));
        assertEquals("7102", merged.get("artio.exitPort"));
    }

    @Test
    void resolveTransportClassNameDefaultsToArtioWhenStartSimulatorWithArtioProvider() {
        ScenarioConfig scenario = ScenarioConfig.builder()
            .simulator(new ScenarioConfig.Simulator("artio", true, null, null, null, null, null, null, null, null))
            .build();

        String resolved = FixReplayCli.resolveTransportClassName(null, true, scenario);
        assertEquals("io.fixreplay.adapter.artio.ArtioFixTransport", resolved);
    }

    @Test
    void resolveTransportClassNameFailsWhenMissingWithoutArtioSimulator() {
        ScenarioConfig scenario = ScenarioConfig.builder()
            .simulator(new ScenarioConfig.Simulator("none", false, null, null, null, null, null, null, null, null))
            .build();

        IllegalArgumentException error = org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> FixReplayCli.resolveTransportClassName(null, false, scenario)
        );
        assertTrue(error.getMessage().contains("--transport-class"));
    }

    public static final class ScriptedTransport implements FixTransport {
        private static final AtomicInteger connectCalls = new AtomicInteger();
        private static volatile List<FixMessage> responses = List.of();
        private static volatile Map<String, String> lastProperties = Map.of();

        private Consumer<FixMessage> callback = ignored -> {};
        private int cursor;

        public static void reset(List<FixMessage> scriptedResponses) {
            responses = List.copyOf(scriptedResponses);
            connectCalls.set(0);
            lastProperties = Map.of();
        }

        @Override
        public void connect(TransportSessionConfig sessionConfig) {
            connectCalls.incrementAndGet();
            lastProperties = sessionConfig.properties();
        }

        @Override
        public void onReceive(Consumer<FixMessage> callback) {
            this.callback = callback;
        }

        @Override
        public void send(FixMessage message) {
            if (cursor < responses.size()) {
                callback.accept(responses.get(cursor++));
            }
        }

        @Override
        public void close() {
            // No-op.
        }
    }
}
