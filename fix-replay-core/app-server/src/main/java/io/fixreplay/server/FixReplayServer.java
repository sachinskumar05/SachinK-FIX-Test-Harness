package io.fixreplay.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fixreplay.runner.FixReplayRunner;
import io.javalin.Javalin;
import java.util.Map;

public final class FixReplayServer {
    private final ObjectMapper objectMapper;
    private final FixReplayRunner runner;

    public FixReplayServer() {
        this(new ObjectMapper(), new FixReplayRunner());
    }

    public FixReplayServer(ObjectMapper objectMapper, FixReplayRunner runner) {
        this.objectMapper = objectMapper;
        this.runner = runner;
    }

    public Javalin start(int port) {
        Javalin app = Javalin.create(config -> config.showJavalinBanner = false);

        app.get("/health", ctx -> ctx.json(Map.of("status", "ok")));
        app.get("/version", ctx -> ctx.json(Map.of("version", currentVersion())));
        app.post("/replay/dry-run", ctx -> {
            ReplayRequest request = objectMapper.readValue(ctx.body(), ReplayRequest.class);
            var result = runner.run(request.expectedLines(), request.actualLines());
            ctx.json(result);
        });

        app.start(port);
        return app;
    }

    String currentVersion() {
        String version = System.getProperty("fix.replay.version");
        return version == null || version.isBlank() ? "0.1.0-SNAPSHOT" : version;
    }

    public static void main(String[] args) {
        new FixReplayServer().start(7000);
    }
}
