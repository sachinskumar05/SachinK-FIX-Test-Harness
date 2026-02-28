package io.fixreplay.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.UploadedFile;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class FixReplayServer implements AutoCloseable {
    private final ObjectMapper objectMapper;
    private final ServerOrchestrator orchestrator;
    private final InMemoryJobStore jobStore;
    private final Path uploadDir;

    private Javalin app;

    public FixReplayServer() {
        this(defaultObjectMapper());
    }

    public FixReplayServer(ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.orchestrator = new ServerOrchestrator(this.objectMapper);
        this.jobStore = new InMemoryJobStore(this.objectMapper);
        this.uploadDir = Path.of(System.getProperty("java.io.tmpdir"), "fix-replay-server-uploads");
    }

    public synchronized Javalin start(int port) {
        if (app != null) {
            throw new IllegalStateException("Server is already running");
        }

        Javalin server = Javalin.create(config -> config.showJavalinBanner = false);
        registerRoutes(server);
        server.start(port);
        app = server;
        return server;
    }

    public synchronized void stop() {
        if (app != null) {
            app.stop();
            app = null;
        }
    }

    @Override
    public void close() {
        stop();
        jobStore.close();
    }

    String currentVersion() {
        String version = System.getProperty("fix.replay.version");
        return version == null || version.isBlank() ? "0.1.0-SNAPSHOT" : version;
    }

    private void registerRoutes(Javalin server) {
        server.get("/health", ctx -> ctx.json(Map.of("status", "ok")));
        server.get("/version", ctx -> ctx.json(Map.of("version", currentVersion())));

        server.post("/api/scan", this::handleScan);
        server.post("/api/prepare", this::handlePrepare);
        server.post("/api/run-offline", this::handleRunOffline);
        server.post("/api/run-online", this::handleRunOnline);
        server.get("/api/job/{id}", this::handleJobStatus);
    }

    private void handleScan(Context ctx) {
        try {
            if (isMultipart(ctx)) {
                UploadedFile uploadedFile = firstUploadedFile(ctx);
                Path uploadedPath = saveUpload(uploadedFile);
                boolean async = parseBoolean(ctx.formParam("async"), false);
                if (async) {
                    submitAsync(ctx, "scan", () -> orchestrator.scan(uploadedPath));
                } else {
                    ctx.json(orchestrator.scan(uploadedPath));
                }
                return;
            }

            ScanRequest request = parseBody(ctx, ScanRequest.class);
            Path scanPath = requiredPath(request.path(), "path");
            if (isAsync(request.async(), false)) {
                submitAsync(ctx, "scan", () -> orchestrator.scan(scanPath));
            } else {
                ctx.json(orchestrator.scan(scanPath));
            }
        } catch (Exception failure) {
            handleFailure(ctx, failure);
        }
    }

    private void handlePrepare(Context ctx) {
        try {
            PrepareRequest request = parseBody(ctx, PrepareRequest.class);
            Path inputFolder = requiredPath(request.inputFolder(), "inputFolder");
            Path expectedFolder = requiredPath(request.expectedFolder(), "expectedFolder");
            Path scenarioPath = requiredPath(request.scenarioPath(), "scenarioPath");
            Path cacheDir = optionalPath(request.cacheDir());

            if (isAsync(request.async(), false)) {
                submitAsync(ctx, "prepare",
                        () -> orchestrator.prepare(inputFolder, expectedFolder, scenarioPath, cacheDir));
            } else {
                ctx.json(orchestrator.prepare(inputFolder, expectedFolder, scenarioPath, cacheDir));
            }
        } catch (Exception failure) {
            handleFailure(ctx, failure);
        }
    }

    private void handleRunOffline(Context ctx) {
        try {
            RunOfflineRequest request = parseBody(ctx, RunOfflineRequest.class);
            Path scenarioPath = requiredPath(request.scenarioPath(), "scenarioPath");
            Path junitPath = optionalPath(request.junitPath());

            if (isAsync(request.async(), false)) {
                submitAsync(ctx, "run-offline", () -> orchestrator.runOffline(scenarioPath, junitPath));
            } else {
                ctx.json(orchestrator.runOffline(scenarioPath, junitPath));
            }
        } catch (Exception failure) {
            handleFailure(ctx, failure);
        }
    }

    private void handleRunOnline(Context ctx) {
        try {
            RunOnlineRequest request = parseBody(ctx, RunOnlineRequest.class);
            ServerOrchestrator.RunOnlineOptions options = new ServerOrchestrator.RunOnlineOptions(
                    requiredPath(request.scenarioPath(), "scenarioPath"),
                    requiredText(request.transportClass(), "transportClass"),
                    request.transportProperties(),
                    optionalPath(request.transportConfigPath()),
                    request.receiveTimeoutMs(),
                    request.queueCapacity(),
                    optionalPath(request.junitPath()));

            if (isAsync(request.async(), true)) {
                submitAsync(ctx, "run-online", () -> orchestrator.runOnline(options));
            } else {
                ctx.json(orchestrator.runOnline(options));
            }
        } catch (Exception failure) {
            handleFailure(ctx, failure);
        }
    }

    private void handleJobStatus(Context ctx) {
        String jobId = ctx.pathParam("id");
        jobStore.get(jobId).ifPresentOrElse(
                snapshot -> ctx.json(snapshot),
                () -> ctx.status(404).json(error("Job not found: " + jobId)));
    }

    private <T> T parseBody(Context ctx, Class<T> type) throws IOException {
        if (ctx.body().isBlank()) {
            throw new IllegalArgumentException("Request body must not be empty");
        }
        return objectMapper.readValue(ctx.body(), type);
    }

    private void submitAsync(Context ctx, String name, ThrowingSupplier supplier) {
        String jobId = jobStore.submit(name, () -> {
            try {
                return supplier.get();
            } catch (Exception failure) {
                throw new IllegalStateException(failure.getMessage(), failure);
            }
        });
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("jobId", jobId);
        response.put("status", "QUEUED");
        ctx.status(202).json(response);
    }

    private void handleFailure(Context ctx, Exception failure) {
        if (failure instanceof IllegalArgumentException ||
                failure instanceof IOException ||
                failure instanceof ReflectiveOperationException) {
            ctx.status(400).json(error(failure.getMessage()));
            return;
        }
        ctx.status(500).json(error("Internal server error"));
    }

    private Map<String, String> error(String message) {
        return Map.of("error", message == null ? "Unknown error" : message);
    }

    private UploadedFile firstUploadedFile(Context ctx) {
        List<UploadedFile> files = ctx.uploadedFiles("file");
        if (files.isEmpty()) {
            files = ctx.uploadedFiles();
        }
        if (files.isEmpty()) {
            throw new IllegalArgumentException("Multipart request must include a file part");
        }
        return files.get(0);
    }

    private Path saveUpload(UploadedFile uploadedFile) throws IOException {
        Files.createDirectories(uploadDir);
        String fileName = sanitizeFileName(uploadedFile.filename());
        Path target = uploadDir.resolve(UUID.randomUUID() + "-" + fileName);
        try (InputStream in = uploadedFile.content()) {
            Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
        }
        return target;
    }

    private boolean isMultipart(Context ctx) {
        String contentType = ctx.contentType();
        return contentType != null && contentType.toLowerCase().contains("multipart/form-data");
    }

    private boolean isAsync(Boolean requested, boolean defaultValue) {
        return requested == null ? defaultValue : requested;
    }

    private boolean parseBoolean(String value, boolean defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }

    private Path requiredPath(String raw, String fieldName) {
        return Path.of(requiredText(raw, fieldName));
    }

    private Path optionalPath(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        return Path.of(raw);
    }

    private String requiredText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " is required");
        }
        return value;
    }

    private String sanitizeFileName(String fileName) {
        if (fileName == null || fileName.isBlank()) {
            return "upload.log";
        }
        return fileName.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    private static ObjectMapper defaultObjectMapper() {
        return com.fasterxml.jackson.databind.json.JsonMapper.builder()
                .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                .enable(com.fasterxml.jackson.databind.MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
                .build();
    }

    public static void main(String[] args) {
        int port = 7000;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        FixReplayServer server = new FixReplayServer();
        server.start(port);
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
    }

    @FunctionalInterface
    private interface ThrowingSupplier {
        Object get() throws Exception;
    }

    private record ScanRequest(String path, Boolean async) {
    }

    private record PrepareRequest(String inputFolder, String expectedFolder, String scenarioPath, String cacheDir,
            Boolean async) {
    }

    private record RunOfflineRequest(String scenarioPath, String junitPath, Boolean async) {
    }

    private record RunOnlineRequest(
            String scenarioPath,
            String transportClass,
            Map<String, String> transportProperties,
            String transportConfigPath,
            Long receiveTimeoutMs,
            Integer queueCapacity,
            String junitPath,
            Boolean async) {
    }
}
