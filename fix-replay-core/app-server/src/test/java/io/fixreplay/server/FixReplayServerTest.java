package io.fixreplay.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fixreplay.model.FixMessage;
import io.fixreplay.runner.FixTransport;
import io.fixreplay.runner.TransportSessionConfig;
import io.javalin.Javalin;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FixReplayServerTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void scanEndpointReturnsSummary(@TempDir Path tempDir) throws Exception {
        Path log = tempDir.resolve("scan.log");
        Files.writeString(
            log,
            String.join(
                "\n",
                "2026-01-01 12:00:00.000 IN [8=FIX.4.4^A35=D^A49=BUY^A56=SELL^A11=ORD-1^A10=001^A]",
                "noise line",
                "2026-01-01 12:00:01.000 OUT 8=FIX.4.4|35=8|49=SELL|56=BUY|37=EX-1|10=002|"
            ) + "\n"
        );

        try (FixReplayServer server = new FixReplayServer()) {
            Javalin app = server.start(0);
            HttpClient client = HttpClient.newHttpClient();

            JsonNode response = postJson(
                client,
                app.port(),
                "/api/scan",
                "{\"path\":\"" + escapeJson(log.toString()) + "\"}"
            );

            assertEquals(2, response.path("messageCount").asInt());
            assertEquals(1, response.path("msgTypeDistribution").path("D").asInt());
            assertEquals(1, response.path("msgTypeDistribution").path("8").asInt());
        }
    }

    @Test
    void prepareEndpointReturnsLinkReport(@TempDir Path tempDir) throws Exception {
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

        try (FixReplayServer server = new FixReplayServer()) {
            Javalin app = server.start(0);
            HttpClient client = HttpClient.newHttpClient();

            String body = "{"
                + "\"inputFolder\":\"" + escapeJson(input.toString()) + "\","
                + "\"expectedFolder\":\"" + escapeJson(expected.toString()) + "\","
                + "\"scenarioPath\":\"" + escapeJson(scenario.toString()) + "\","
                + "\"cacheDir\":\"" + escapeJson(cache.toString()) + "\""
                + "}";
            JsonNode response = postJson(client, app.port(), "/api/prepare", body);

            assertEquals(1, response.path("counts").path("sessions").asInt());
            assertTrue(response.path("linkReports").has("BUY_SELL"));
            assertTrue(Files.exists(cache.resolve("link-report.json")));
        }
    }

    @Test
    void runOfflineEndpointReturnsDiffReport(@TempDir Path tempDir) throws Exception {
        Path input = Files.createDirectory(tempDir.resolve("input"));
        Path expected = Files.createDirectory(tempDir.resolve("expected"));
        Path actual = Files.createDirectory(tempDir.resolve("actual"));
        Path scenario = tempDir.resolve("scenario.yaml");

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

        try (FixReplayServer server = new FixReplayServer()) {
            Javalin app = server.start(0);
            HttpClient client = HttpClient.newHttpClient();

            JsonNode response = postJson(
                client,
                app.port(),
                "/api/run-offline",
                "{\"scenarioPath\":\"" + escapeJson(scenario.toString()) + "\"}"
            );

            assertTrue(response.path("diffReport").path("failedMessages").asInt() >= 1);
            assertTrue(response.has("linkReports"));
        }
    }

    @Test
    void runOnlineAsyncJobCanBePolled(@TempDir Path tempDir) throws Exception {
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
                "msgTypeFilter: [D,8]"
            ) + "\n"
        );

        ScriptedTransport.reset(List.of(FixMessage.fromRaw("8=FIX.4.4|35=D|11=ORD-1|55=MSFT|10=099|", '|')));

        try (FixReplayServer server = new FixReplayServer()) {
            Javalin app = server.start(0);
            HttpClient client = HttpClient.newHttpClient();

            String body = "{"
                + "\"scenarioPath\":\"" + escapeJson(scenario.toString()) + "\","
                + "\"transportClass\":\"" + escapeJson(ScriptedTransport.class.getName()) + "\","
                + "\"transportProperties\":{\"custom\":\"value\"},"
                + "\"async\":true"
                + "}";
            HttpResponse<String> submitted = postJsonRaw(client, app.port(), "/api/run-online", body);
            assertEquals(202, submitted.statusCode());

            JsonNode submittedJson = JSON.readTree(submitted.body());
            String jobId = submittedJson.path("jobId").asText();
            JsonNode job = awaitCompletion(client, app.port(), jobId);

            assertEquals("SUCCEEDED", job.path("status").asText());
            assertTrue(job.path("result").has("diffReport"));
            assertEquals(1, ScriptedTransport.connectCalls.get());
            assertEquals("value", ScriptedTransport.lastProperties.get("custom"));
        }
    }

    private static JsonNode awaitCompletion(HttpClient client, int port, String jobId) throws Exception {
        for (int i = 0; i < 40; i++) {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/job/" + jobId))
                .timeout(Duration.ofSeconds(2))
                .GET()
                .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            assertEquals(200, response.statusCode());
            JsonNode body = JSON.readTree(response.body());
            String status = body.path("status").asText();
            if ("SUCCEEDED".equals(status) || "FAILED".equals(status)) {
                return body;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("Job did not complete in time");
    }

    private static JsonNode postJson(HttpClient client, int port, String path, String body) throws Exception {
        HttpResponse<String> response = postJsonRaw(client, port, path, body);
        assertEquals(200, response.statusCode(), response.body());
        return JSON.readTree(response.body());
    }

    private static HttpResponse<String> postJsonRaw(HttpClient client, int port, String path, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + path))
            .timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
            .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private static String escapeJson(String raw) {
        return raw.replace("\\", "\\\\").replace("\"", "\\\"");
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

