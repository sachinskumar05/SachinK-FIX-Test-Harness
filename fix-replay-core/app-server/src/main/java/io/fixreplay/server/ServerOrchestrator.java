package io.fixreplay.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fixreplay.compare.CompareResult;
import io.fixreplay.compare.DiffReport;
import io.fixreplay.linker.LinkReport;
import io.fixreplay.loader.FixLogScanner;
import io.fixreplay.loader.FixRawMessage;
import io.fixreplay.model.FixMessage;
import io.fixreplay.model.MsgTypeFilter;
import io.fixreplay.runner.FixTransport;
import io.fixreplay.runner.OfflineRunResult;
import io.fixreplay.runner.OfflineRunner;
import io.fixreplay.runner.OnlineRunResult;
import io.fixreplay.runner.OnlineRunner;
import io.fixreplay.runner.OnlineRunnerConfig;
import io.fixreplay.runner.ScenarioConfig;
import io.fixreplay.runner.SessionKey;
import io.fixreplay.runner.TransportSessionConfig;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Stream;

final class ServerOrchestrator {
    private static final long DEFAULT_RECEIVE_TIMEOUT_MS = 5_000L;
    private static final int DEFAULT_QUEUE_CAPACITY = 1_024;

    private final ObjectMapper objectMapper;
    private final FixLogScanner scanner;

    ServerOrchestrator(ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.scanner = new FixLogScanner();
    }

    Map<String, Object> scan(Path path) throws IOException {
        List<Path> files = collectInputFiles(path);
        long messageCount = 0L;
        Map<String, Integer> msgTypeDistribution = new TreeMap<>();
        Map<String, Integer> sessions = new TreeMap<>();

        for (Path file : files) {
            try (Stream<FixRawMessage> messages = scanner.scan(file)) {
                for (FixRawMessage raw : (Iterable<FixRawMessage>) messages::iterator) {
                    messageCount++;
                    FixMessage message = raw.toFixMessage();
                    msgTypeDistribution.merge(defaultString(message.msgType(), "UNKNOWN"), 1, Integer::sum);
                    sessions.merge(resolveSessionId(message), 1, Integer::sum);
                }
            }
        }

        Map<String, Object> report = new LinkedHashMap<>();
        report.put("path", normalizePath(path));
        report.put("filesScanned", files.size());
        report.put("messageCount", messageCount);
        report.put("msgTypeDistribution", msgTypeDistribution);
        report.put("sessionsDetected", toSessionCounts(sessions));
        return report;
    }

    Map<String, Object> prepare(Path inputFolder, Path expectedFolder, Path scenarioPath, Path cacheDir) throws IOException {
        ScenarioConfig base = ScenarioConfig.load(scenarioPath);
        ScenarioConfig effective = copyScenarioWithFolders(base, inputFolder, expectedFolder, null);

        OfflineRunResult result = new OfflineRunner().run(effective);
        Map<String, JsonNode> linkReports = parseAndSortLinkReports(result.linkReports());

        int matched = 0;
        int unmatched = 0;
        int ambiguous = 0;
        for (LinkReport report : result.linkReports().values()) {
            matched += report.matched();
            unmatched += report.unmatched();
            ambiguous += report.ambiguous();
        }

        Map<String, Object> counts = new LinkedHashMap<>();
        counts.put("sessions", linkReports.size());
        counts.put("matched", matched);
        counts.put("unmatched", unmatched);
        counts.put("ambiguous", ambiguous);

        Map<String, Object> output = new LinkedHashMap<>();
        output.put("scenario", normalizePath(scenarioPath));
        output.put("inputFolder", normalizePath(inputFolder));
        output.put("expectedFolder", normalizePath(expectedFolder));
        output.put("counts", counts);
        output.put("linkReports", linkReports);

        if (cacheDir != null) {
            Files.createDirectories(cacheDir);
            writeJson(cacheDir.resolve("link-report.json"), output);
            for (Map.Entry<String, JsonNode> entry : linkReports.entrySet()) {
                writeJson(cacheDir.resolve("link-report-" + sanitizeFilePart(entry.getKey()) + ".json"), entry.getValue());
            }
        }

        return output;
    }

    Map<String, Object> runOffline(Path scenarioPath, Path junitPath) throws IOException {
        ScenarioConfig scenarioConfig = ScenarioConfig.load(scenarioPath);
        OfflineRunResult result = new OfflineRunner().run(scenarioConfig);

        DiffReport effectiveDiff = withOperationalFailures(
            result.diffReport(),
            "offline",
            result.unmatchedExpected(),
            result.unmatchedActual(),
            result.ambiguous(),
            false,
            0
        );

        if (junitPath != null) {
            writeText(junitPath, effectiveDiff.toJUnitXml("run-offline"));
        }
        return buildOfflineOutput(result, effectiveDiff);
    }

    Map<String, Object> runOnline(RunOnlineOptions options) throws IOException, ReflectiveOperationException {
        ScenarioConfig scenarioConfig = ScenarioConfig.load(options.scenarioPath());
        Map<String, String> mergedTransportProperties = mergeTransportProperties(
            options.transportConfigPath(),
            options.transportProperties()
        );

        Map<SessionKey, Path> inputFiles = scenarioConfig.resolveInputFiles();
        Map<SessionKey, Path> expectedFiles = scenarioConfig.resolveExpectedFiles();
        List<SessionKey> sessions = new ArrayList<>(expectedFiles.keySet());
        sessions.sort(Comparator.comparing(SessionKey::id));

        if (sessions.isEmpty()) {
            throw new IllegalArgumentException("Scenario has no expected .out files");
        }

        List<DiffReport.MessageResult> allDiffMessages = new ArrayList<>();
        Map<String, JsonNode> linkReports = new TreeMap<>();
        List<Map<String, Object>> perSession = new ArrayList<>();

        int totalSent = 0;
        int totalReceived = 0;
        int totalDropped = 0;
        int totalMatched = 0;
        int totalUnmatchedExpected = 0;
        int totalUnmatchedActual = 0;
        int totalAmbiguous = 0;
        int timedOutSessions = 0;
        boolean overallPass = true;

        OnlineRunner runner = new OnlineRunner();
        for (SessionKey session : sessions) {
            Path inputPath = inputFiles.get(session);
            Path expectedPath = expectedFiles.get(session);
            if (inputPath == null) {
                throw new IllegalArgumentException("Missing input .in file for session " + session.id());
            }

            List<FixMessage> entryMessages = loadMessages(inputPath, scenarioConfig.msgTypeFilter());
            List<FixMessage> expectedMessages = loadMessages(expectedPath, scenarioConfig.msgTypeFilter());

            FixTransport transport = instantiateTransport(options.transportClassName());
            TransportSessionConfig transportSessionConfig = new TransportSessionConfig(
                session,
                reverseSession(session),
                mergedTransportProperties
            );
            OnlineRunnerConfig runnerConfig = new OnlineRunnerConfig(
                scenarioConfig.linkerConfig(),
                scenarioConfig.compareConfig(),
                scenarioConfig.msgTypeFilter(),
                Duration.ofMillis(options.receiveTimeoutMs()),
                options.queueCapacity()
            );

            OnlineRunResult result = runner.run(
                entryMessages,
                expectedMessages,
                transport,
                transportSessionConfig,
                runnerConfig
            );

            totalSent += result.sentCount();
            totalReceived += result.receivedCount();
            totalDropped += result.droppedCount();
            totalMatched += result.matchedComparisons();
            totalUnmatchedExpected += result.unmatchedExpected();
            totalUnmatchedActual += result.unmatchedActual();
            totalAmbiguous += result.ambiguous();
            if (result.timedOut()) {
                timedOutSessions++;
            }

            DiffReport sessionDiff = withOperationalFailures(
                result.diffReport(),
                session.id(),
                result.unmatchedExpected(),
                result.unmatchedActual(),
                result.ambiguous(),
                result.timedOut(),
                result.droppedCount()
            );
            for (DiffReport.MessageResult message : sessionDiff.messages()) {
                allDiffMessages.add(new DiffReport.MessageResult(session.id() + ":" + message.id(), message.result()));
            }
            linkReports.put(session.id(), parseJson(result.linkReport().toJson()));

            boolean sessionPass = result.passed() && !result.timedOut();
            overallPass = overallPass && sessionPass;

            Map<String, Object> sessionSummary = new LinkedHashMap<>();
            sessionSummary.put("session", session.id());
            sessionSummary.put("sent", result.sentCount());
            sessionSummary.put("received", result.receivedCount());
            sessionSummary.put("dropped", result.droppedCount());
            sessionSummary.put("timedOut", result.timedOut());
            sessionSummary.put("matchedComparisons", result.matchedComparisons());
            sessionSummary.put("unmatchedExpected", result.unmatchedExpected());
            sessionSummary.put("unmatchedActual", result.unmatchedActual());
            sessionSummary.put("ambiguous", result.ambiguous());
            sessionSummary.put("passed", sessionPass);
            perSession.add(sessionSummary);
        }

        DiffReport diffReport = new DiffReport(allDiffMessages);
        if (options.junitPath() != null) {
            writeText(options.junitPath(), diffReport.toJUnitXml("run-online"));
        }

        Map<String, Object> counts = new LinkedHashMap<>();
        counts.put("sessions", sessions.size());
        counts.put("timedOutSessions", timedOutSessions);
        counts.put("sent", totalSent);
        counts.put("received", totalReceived);
        counts.put("dropped", totalDropped);
        counts.put("matchedComparisons", totalMatched);
        counts.put("unmatchedExpected", totalUnmatchedExpected);
        counts.put("unmatchedActual", totalUnmatchedActual);
        counts.put("ambiguous", totalAmbiguous);

        Map<String, Object> report = new LinkedHashMap<>();
        report.put("passed", overallPass);
        report.put("scenario", normalizePath(options.scenarioPath()));
        report.put("counts", counts);
        report.put("sessions", perSession);
        report.put("diffReport", parseJson(diffReport.toJson()));
        report.put("linkReports", linkReports);
        return report;
    }

    private Map<String, Object> buildOfflineOutput(OfflineRunResult result, DiffReport effectiveDiff) throws IOException {
        Map<String, Object> counts = new LinkedHashMap<>();
        counts.put("matchedComparisons", result.matchedComparisons());
        counts.put("unmatchedExpected", result.unmatchedExpected());
        counts.put("unmatchedActual", result.unmatchedActual());
        counts.put("ambiguous", result.ambiguous());

        Map<String, Object> output = new LinkedHashMap<>();
        output.put("passed", result.passed());
        output.put("usedActualMessages", result.usedActualMessages());
        output.put("counts", counts);
        output.put("diffReport", parseJson(effectiveDiff.toJson()));
        output.put("linkReports", parseAndSortLinkReports(result.linkReports()));
        return output;
    }

    private Map<String, JsonNode> parseAndSortLinkReports(Map<SessionKey, LinkReport> source) throws IOException {
        Map<String, JsonNode> sorted = new TreeMap<>();
        for (Map.Entry<SessionKey, LinkReport> entry : source.entrySet()) {
            sorted.put(entry.getKey().id(), parseJson(entry.getValue().toJson()));
        }
        return sorted;
    }

    private DiffReport withOperationalFailures(
        DiffReport base,
        String scope,
        int unmatchedExpected,
        int unmatchedActual,
        int ambiguous,
        boolean timedOut,
        int dropped
    ) {
        if (
            unmatchedExpected <= 0 &&
            unmatchedActual <= 0 &&
            ambiguous <= 0 &&
            dropped <= 0 &&
            !timedOut
        ) {
            return base;
        }

        List<DiffReport.MessageResult> messages = new ArrayList<>(base.messages());
        if (unmatchedExpected > 0) {
            messages.add(operationalFailure(scope + ":unmatched-expected", "unmatchedExpected", unmatchedExpected));
        }
        if (unmatchedActual > 0) {
            messages.add(operationalFailure(scope + ":unmatched-actual", "unmatchedActual", unmatchedActual));
        }
        if (ambiguous > 0) {
            messages.add(operationalFailure(scope + ":ambiguous", "ambiguous", ambiguous));
        }
        if (dropped > 0) {
            messages.add(operationalFailure(scope + ":dropped", "dropped", dropped));
        }
        if (timedOut) {
            messages.add(operationalFailure(scope + ":timeout", "timedOut", 1));
        }
        return new DiffReport(messages);
    }

    private DiffReport.MessageResult operationalFailure(String id, String field, int value) {
        CompareResult failure = new CompareResult(
            "SYSTEM",
            List.of(),
            List.of(),
            Map.of(9000, new CompareResult.ValueDifference(field + "=0", field + "=" + value))
        );
        return new DiffReport.MessageResult(id, failure);
    }

    private Map<String, String> mergeTransportProperties(Path configFile, Map<String, String> requestProperties) throws IOException {
        Map<String, String> merged = new LinkedHashMap<>();
        if (configFile != null) {
            merged.putAll(loadTransportConfig(configFile));
        }
        if (requestProperties != null) {
            merged.putAll(requestProperties);
        }
        return Map.copyOf(merged);
    }

    private Map<String, String> loadTransportConfig(Path configFile) throws IOException {
        if (!Files.isRegularFile(configFile)) {
            throw new IllegalArgumentException("Transport config file does not exist: " + configFile);
        }

        String lower = configFile.getFileName().toString().toLowerCase(Locale.ROOT);
        if (lower.endsWith(".json")) {
            try (InputStream in = Files.newInputStream(configFile)) {
                Map<String, String> parsed = objectMapper.readValue(in, new TypeReference<>() {});
                return Map.copyOf(parsed);
            }
        }

        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(configFile)) {
            properties.load(in);
        }
        Map<String, String> parsed = new LinkedHashMap<>();
        for (String name : properties.stringPropertyNames()) {
            parsed.put(name, properties.getProperty(name));
        }
        return Map.copyOf(parsed);
    }

    private ScenarioConfig copyScenarioWithFolders(
        ScenarioConfig base,
        Path inputFolder,
        Path expectedFolder,
        Path actualFolder
    ) {
        ScenarioConfig.Builder builder = ScenarioConfig.builder()
            .inputFolder(inputFolder)
            .expectedFolder(expectedFolder)
            .msgTypeFilter(base.msgTypeFilter())
            .linkerConfig(base.linkerConfig())
            .compareConfig(base.compareConfig())
            .sessionMappingRules(base.sessionMappingRules());
        if (actualFolder != null) {
            builder.actualFolder(actualFolder);
        }
        return builder.build();
    }

    private List<FixMessage> loadMessages(Path path, MsgTypeFilter filter) {
        if (path == null) {
            return List.of();
        }
        try (Stream<FixRawMessage> messages = scanner.scan(path)) {
            return messages
                .map(FixRawMessage::toFixMessage)
                .filter(filter::accepts)
                .toList();
        }
    }

    private List<Path> collectInputFiles(Path path) throws IOException {
        if (Files.isRegularFile(path)) {
            return List.of(path);
        }
        if (!Files.isDirectory(path)) {
            throw new IllegalArgumentException("Input path does not exist or is not a file/directory: " + path);
        }
        try (Stream<Path> stream = Files.walk(path)) {
            return stream
                .filter(Files::isRegularFile)
                .sorted(Comparator.comparing(Path::toString))
                .toList();
        }
    }

    private List<Map<String, Object>> toSessionCounts(Map<String, Integer> sessions) {
        List<Map<String, Object>> result = new ArrayList<>(sessions.size());
        for (Map.Entry<String, Integer> entry : sessions.entrySet()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("session", entry.getKey());
            item.put("count", entry.getValue());
            result.add(item);
        }
        return result;
    }

    private String resolveSessionId(FixMessage message) {
        String sender = message.senderCompId();
        String target = message.targetCompId();
        if (sender == null || target == null || sender.isBlank() || target.isBlank()) {
            return "UNKNOWN";
        }
        return sender + "_" + target;
    }

    private String defaultString(String value, String fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return value;
    }

    private String sanitizeFilePart(String value) {
        return value.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    private String normalizePath(Path path) {
        return path.toAbsolutePath().normalize().toString();
    }

    private SessionKey reverseSession(SessionKey session) {
        return new SessionKey(session.targetCompId(), session.senderCompId());
    }

    private FixTransport instantiateTransport(String className) throws ReflectiveOperationException {
        Class<?> type = Class.forName(Objects.requireNonNull(className, "transportClass"));
        if (!FixTransport.class.isAssignableFrom(type)) {
            throw new IllegalArgumentException(className + " does not implement " + FixTransport.class.getName());
        }
        return (FixTransport) type.getDeclaredConstructor().newInstance();
    }

    private JsonNode parseJson(String json) throws IOException {
        return objectMapper.readTree(json);
    }

    private void writeJson(Path path, Object value) throws IOException {
        Path parent = path.toAbsolutePath().normalize().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        objectMapper.writeValue(path.toFile(), value);
    }

    private void writeText(Path path, String value) throws IOException {
        Path parent = path.toAbsolutePath().normalize().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(path, value);
    }

    static final class RunOnlineOptions {
        private final Path scenarioPath;
        private final String transportClassName;
        private final Map<String, String> transportProperties;
        private final Path transportConfigPath;
        private final long receiveTimeoutMs;
        private final int queueCapacity;
        private final Path junitPath;

        RunOnlineOptions(
            Path scenarioPath,
            String transportClassName,
            Map<String, String> transportProperties,
            Path transportConfigPath,
            Long receiveTimeoutMs,
            Integer queueCapacity,
            Path junitPath
        ) {
            this.scenarioPath = Objects.requireNonNull(scenarioPath, "scenarioPath");
            this.transportClassName = Objects.requireNonNull(transportClassName, "transportClassName");
            this.transportProperties = transportProperties == null ? Map.of() : Map.copyOf(transportProperties);
            this.transportConfigPath = transportConfigPath;
            this.receiveTimeoutMs = receiveTimeoutMs == null ? DEFAULT_RECEIVE_TIMEOUT_MS : receiveTimeoutMs;
            this.queueCapacity = queueCapacity == null ? DEFAULT_QUEUE_CAPACITY : queueCapacity;
            this.junitPath = junitPath;

            if (this.receiveTimeoutMs <= 0) {
                throw new IllegalArgumentException("receiveTimeoutMs must be > 0");
            }
            if (this.queueCapacity <= 0) {
                throw new IllegalArgumentException("queueCapacity must be > 0");
            }
        }

        Path scenarioPath() {
            return scenarioPath;
        }

        String transportClassName() {
            return transportClassName;
        }

        Map<String, String> transportProperties() {
            return transportProperties;
        }

        Path transportConfigPath() {
            return transportConfigPath;
        }

        long receiveTimeoutMs() {
            return receiveTimeoutMs;
        }

        int queueCapacity() {
            return queueCapacity;
        }

        Path junitPath() {
            return junitPath;
        }
    }
}

