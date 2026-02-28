package io.fixreplay.simulator.artio;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public final class ArtioSimulatorConfig {
    private static final String DEFAULT_PROVIDER = "artio";
    private static final String DEFAULT_BEGIN_STRING = "FIX.4.2";
    private static final String DEFAULT_HOST = "0.0.0.0";
    private static final String DEFAULT_LOCAL_COMP_ID = "FIX_GATEWAY";
    private static final String DEFAULT_ENTRY_REMOTE_COMP_ID = "ENTRY_RACOMPID";
    private static final String DEFAULT_EXIT_REMOTE_COMP_ID = "EXIT_RACOMPID";
    private static final List<String> DEFAULT_ENABLED_MSG_TYPES = List.of("D", "G", "F", "8", "3", "j");
    private static final int DEFAULT_MAX_QUEUE_DEPTH = 50_000;
    private static final long DEFAULT_GRACEFUL_TIMEOUT_MS = 5_000L;
    private static final int DEFAULT_FRAGMENT_LIMIT = 50;
    private static final String DEFAULT_IDLE_STRATEGY = "backoff";

    private final boolean enabled;
    private final String provider;
    private final String beginString;
    private final SessionEndpoint entry;
    private final SessionEndpoint exit;
    private final Routing routing;
    private final Mutation mutation;
    private final Observability observability;
    private final Shutdown shutdown;
    private final StorageDirs storageDirs;
    private final Performance performance;

    private ArtioSimulatorConfig(
        boolean enabled,
        String provider,
        String beginString,
        SessionEndpoint entry,
        SessionEndpoint exit,
        Routing routing,
        Mutation mutation,
        Observability observability,
        Shutdown shutdown,
        StorageDirs storageDirs,
        Performance performance
    ) {
        this.enabled = enabled;
        this.provider = Objects.requireNonNull(provider, "provider");
        this.beginString = Objects.requireNonNull(beginString, "beginString");
        this.entry = Objects.requireNonNull(entry, "entry");
        this.exit = Objects.requireNonNull(exit, "exit");
        this.routing = Objects.requireNonNull(routing, "routing");
        this.mutation = Objects.requireNonNull(mutation, "mutation");
        this.observability = Objects.requireNonNull(observability, "observability");
        this.shutdown = Objects.requireNonNull(shutdown, "shutdown");
        this.storageDirs = Objects.requireNonNull(storageDirs, "storageDirs");
        this.performance = Objects.requireNonNull(performance, "performance");
    }

    public static ArtioSimulatorConfig load(Path scenarioPath) throws IOException {
        Objects.requireNonNull(scenarioPath, "scenarioPath");

        ObjectMapper mapper = mapperFor(scenarioPath);
        JsonNode root = mapper.readTree(scenarioPath.toFile());
        if (root == null || root instanceof NullNode) {
            throw new IllegalArgumentException("Scenario config is empty: " + scenarioPath);
        }

        Path scenarioBase = scenarioPath.toAbsolutePath().normalize().getParent();
        if (scenarioBase == null) {
            scenarioBase = Path.of(".").toAbsolutePath().normalize();
        }

        JsonNode simulator = root.path("simulator");
        String provider = textOrDefault(simulator, DEFAULT_PROVIDER, "provider").toLowerCase(Locale.ROOT);
        boolean enabled = booleanOrDefault(simulator, false, "enabled");
        String beginString = textOrDefault(simulator, DEFAULT_BEGIN_STRING, "begin_string", "beginString");

        JsonNode sessions = root.path("sessions");
        String defaultEntryRemoteCompId = firstNonBlank(
            text(sessions.path("entry"), "sender_comp_id", "senderCompId"),
            DEFAULT_ENTRY_REMOTE_COMP_ID
        );
        String defaultExitRemoteCompId = firstNonBlank(
            text(sessions.path("exit"), "target_comp_id", "targetCompId"),
            DEFAULT_EXIT_REMOTE_COMP_ID
        );

        JsonNode entryNode = simulator.path("entry");
        JsonNode exitNode = simulator.path("exit");

        SessionEndpoint entry = new SessionEndpoint(
            textOrDefault(entryNode, DEFAULT_HOST, "listen_host", "listenHost", "host"),
            intOrDefault(entryNode, 0, "listen_port", "listenPort", "port"),
            textOrDefault(
                entryNode,
                DEFAULT_LOCAL_COMP_ID,
                "local_comp_id",
                "localCompId",
                "sender_comp_id",
                "senderCompId"
            ),
            firstNonBlank(
                text(entryNode, "remote_comp_id", "remoteCompId", "target_comp_id", "targetCompId"),
                defaultEntryRemoteCompId
            )
        );
        SessionEndpoint exit = new SessionEndpoint(
            textOrDefault(exitNode, DEFAULT_HOST, "listen_host", "listenHost", "host"),
            intOrDefault(exitNode, 0, "listen_port", "listenPort", "port"),
            textOrDefault(
                exitNode,
                DEFAULT_LOCAL_COMP_ID,
                "local_comp_id",
                "localCompId",
                "sender_comp_id",
                "senderCompId"
            ),
            firstNonBlank(
                text(exitNode, "remote_comp_id", "remoteCompId", "target_comp_id", "targetCompId"),
                defaultExitRemoteCompId
            )
        );

        JsonNode routingNode = simulator.path("routing");
        Set<String> enabledMsgTypes = normalizeMsgTypes(stringList(routingNode, "enabled_msg_types", "enabledMsgTypes"));
        if (enabledMsgTypes.isEmpty()) {
            enabledMsgTypes = new LinkedHashSet<>(DEFAULT_ENABLED_MSG_TYPES);
        }
        Routing routing = new Routing(
            Set.copyOf(enabledMsgTypes),
            booleanOrDefault(routingNode, true, "drop_admin_messages", "dropAdminMessages"),
            longOrDefault(routingNode, 0L, "artificial_delay_ms", "artificialDelayMs"),
            booleanOrDefault(
                routingNode,
                true,
                "fail_if_exit_not_logged_on",
                "failIfExitNotLoggedOn"
            ),
            intOrDefault(routingNode, DEFAULT_MAX_QUEUE_DEPTH, "max_queue_depth", "maxQueueDepth")
        );

        JsonNode mutationNode = simulator.path("mutation");
        JsonNode inlineRules = node(mutationNode, "rules_inline", "rulesInline");
        JsonNode inlineRulesCopy = (inlineRules == null || inlineRules.isMissingNode() || inlineRules.isNull())
            ? NullNode.instance
            : inlineRules.deepCopy();
        Path rulesFile = optionalResolvedPath(scenarioBase, text(mutationNode, "rules_file", "rulesFile"));
        Mutation mutation = new Mutation(
            booleanOrDefault(mutationNode, false, "enabled"),
            booleanOrDefault(mutationNode, false, "strict_mode", "strictMode"),
            inlineRulesCopy,
            rulesFile
        );

        JsonNode observabilityNode = simulator.path("observability");
        Observability observability = new Observability(
            booleanOrDefault(observabilityNode, true, "log_inbound_outbound", "logInboundOutbound"),
            booleanOrDefault(observabilityNode, false, "log_fix_payloads", "logFixPayloads")
        );

        JsonNode shutdownNode = simulator.path("shutdown");
        Shutdown shutdown = new Shutdown(
            longOrDefault(shutdownNode, DEFAULT_GRACEFUL_TIMEOUT_MS, "graceful_timeout_ms", "gracefulTimeoutMs")
        );

        JsonNode artioNode = simulator.path("artio");
        Path defaultWorkDir = defaultWorkDir(root, scenarioBase);
        Path workDir = optionalResolvedPath(scenarioBase, text(artioNode, "work_dir", "workDir"));
        if (workDir == null) {
            workDir = defaultWorkDir;
        }
        Path aeronDir = optionalResolvedPath(scenarioBase, text(artioNode, "aeron_dir", "aeronDir"));
        if (aeronDir == null) {
            aeronDir = workDir.resolve("aeron");
        }
        Path logDir = optionalResolvedPath(scenarioBase, text(artioNode, "log_dir", "logDir"));
        if (logDir == null) {
            logDir = workDir.resolve("logs");
        }
        boolean cleanupOnStop = booleanOrDefault(
            artioNode,
            false,
            "cleanup_on_stop",
            "cleanupOnStop",
            "cleanup_work_dirs_on_stop",
            "cleanupWorkDirsOnStop"
        );
        StorageDirs storageDirs = new StorageDirs(workDir, aeronDir, logDir, cleanupOnStop);

        JsonNode performanceNode = node(artioNode, "performance");
        Performance performance = new Performance(
            intOrDefault(
                performanceNode,
                DEFAULT_FRAGMENT_LIMIT,
                "inbound_fragment_limit",
                "inboundFragmentLimit"
            ),
            intOrDefault(
                performanceNode,
                DEFAULT_FRAGMENT_LIMIT,
                "outbound_fragment_limit",
                "outboundFragmentLimit"
            ),
            textOrDefault(performanceNode, DEFAULT_IDLE_STRATEGY, "idle_strategy", "idleStrategy")
        );

        ArtioSimulatorConfig config = new ArtioSimulatorConfig(
            enabled,
            provider,
            beginString,
            entry,
            exit,
            routing,
            mutation,
            observability,
            shutdown,
            storageDirs,
            performance
        );
        config.validate();
        return config;
    }

    public boolean enabled() {
        return enabled;
    }

    public String provider() {
        return provider;
    }

    public String beginString() {
        return beginString;
    }

    public SessionEndpoint entry() {
        return entry;
    }

    public SessionEndpoint exit() {
        return exit;
    }

    public Routing routing() {
        return routing;
    }

    public Mutation mutation() {
        return mutation;
    }

    public Observability observability() {
        return observability;
    }

    public Shutdown shutdown() {
        return shutdown;
    }

    public StorageDirs storageDirs() {
        return storageDirs;
    }

    public Performance performance() {
        return performance;
    }

    public boolean providerSupportedByArtioModule() {
        return "artio".equalsIgnoreCase(provider) || "quickfixj".equalsIgnoreCase(provider);
    }

    public ArtioSimulatorConfig withOverrides(Integer overrideEntryPort, Integer overrideExitPort, Path overrideRulesFile) {
        SessionEndpoint overriddenEntry = entry;
        if (overrideEntryPort != null) {
            overriddenEntry = new SessionEndpoint(
                entry.listenHost(),
                overrideEntryPort,
                entry.localCompId(),
                entry.remoteCompId()
            );
        }

        SessionEndpoint overriddenExit = exit;
        if (overrideExitPort != null) {
            overriddenExit = new SessionEndpoint(
                exit.listenHost(),
                overrideExitPort,
                exit.localCompId(),
                exit.remoteCompId()
            );
        }

        Mutation overriddenMutation = mutation;
        if (overrideRulesFile != null) {
            overriddenMutation = new Mutation(
                mutation.enabled(),
                mutation.strictMode(),
                NullNode.instance,
                overrideRulesFile.toAbsolutePath().normalize()
            );
        }

        ArtioSimulatorConfig overridden = new ArtioSimulatorConfig(
            enabled,
            provider,
            beginString,
            overriddenEntry,
            overriddenExit,
            routing,
            overriddenMutation,
            observability,
            shutdown,
            storageDirs,
            performance
        );
        overridden.validate();
        return overridden;
    }

    private void validate() {
        if (beginString.isBlank()) {
            throw new IllegalArgumentException("simulator.begin_string must not be blank");
        }
        if (routing.maxQueueDepth() <= 0) {
            throw new IllegalArgumentException("simulator.routing.max_queue_depth must be > 0");
        }
        if (routing.artificialDelayMs() < 0) {
            throw new IllegalArgumentException("simulator.routing.artificial_delay_ms must be >= 0");
        }
        if (shutdown.gracefulTimeoutMs() <= 0) {
            throw new IllegalArgumentException("simulator.shutdown.graceful_timeout_ms must be > 0");
        }
        if (performance.inboundFragmentLimit() <= 0 || performance.outboundFragmentLimit() <= 0) {
            throw new IllegalArgumentException("simulator.artio.performance fragment limits must be > 0");
        }
        if (performance.idleStrategy().isBlank()) {
            throw new IllegalArgumentException("simulator.artio.performance.idle_strategy must not be blank");
        }

        if (!enabled) {
            return;
        }

        if (entry.listenPort() <= 0) {
            throw new IllegalArgumentException("simulator.entry.listen_port must be set and > 0 when simulator.enabled=true");
        }
        if (exit.listenPort() <= 0) {
            throw new IllegalArgumentException("simulator.exit.listen_port must be set and > 0 when simulator.enabled=true");
        }
        requireNonBlank(entry.listenHost(), "simulator.entry.listen_host");
        requireNonBlank(exit.listenHost(), "simulator.exit.listen_host");
        if (entry.listenPort() == exit.listenPort() && !entry.listenHost().equals(exit.listenHost())) {
            throw new IllegalArgumentException(
                "Single-port topology requires simulator.entry.listen_host and simulator.exit.listen_host to match"
            );
        }
        requireNonBlank(entry.localCompId(), "simulator.entry.local_comp_id");
        requireNonBlank(entry.remoteCompId(), "simulator.entry.remote_comp_id");
        requireNonBlank(exit.localCompId(), "simulator.exit.local_comp_id");
        requireNonBlank(exit.remoteCompId(), "simulator.exit.remote_comp_id");
        if (routing.enabledMsgTypes().isEmpty()) {
            throw new IllegalArgumentException("simulator.routing.enabled_msg_types must not be empty");
        }
    }

    private static void requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }

    private static Path defaultWorkDir(JsonNode root, Path scenarioBase) {
        String cacheFolder = text(root, "cache_folder", "cacheFolder");
        if (cacheFolder != null && !cacheFolder.isBlank()) {
            return resolvePath(scenarioBase, cacheFolder);
        }
        return scenarioBase.resolve(".simulator-artio").normalize();
    }

    private static ObjectMapper mapperFor(Path configPath) {
        String lower = configPath.getFileName().toString().toLowerCase(Locale.ROOT);
        if (lower.endsWith(".yaml") || lower.endsWith(".yml")) {
            return new ObjectMapper(new YAMLFactory());
        }
        return new ObjectMapper();
    }

    private static String firstNonBlank(String... candidates) {
        for (String candidate : candidates) {
            if (candidate != null && !candidate.isBlank()) {
                return candidate;
            }
        }
        return null;
    }

    private static JsonNode node(JsonNode parent, String... names) {
        if (parent == null || parent.isMissingNode() || parent.isNull()) {
            return NullNode.instance;
        }
        for (String name : names) {
            JsonNode value = parent.get(name);
            if (value != null && !value.isNull()) {
                return value;
            }
        }
        return NullNode.instance;
    }

    private static String text(JsonNode parent, String... names) {
        JsonNode value = node(parent, names);
        if (value.isMissingNode() || value.isNull()) {
            return null;
        }
        if (value.isTextual()) {
            String trimmed = value.asText().trim();
            return trimmed.isEmpty() ? null : trimmed;
        }
        if (value.isNumber() || value.isBoolean()) {
            return value.asText();
        }
        return null;
    }

    private static String textOrDefault(JsonNode parent, String defaultValue, String... names) {
        String value = text(parent, names);
        return value == null ? defaultValue : value;
    }

    private static boolean booleanOrDefault(JsonNode parent, boolean defaultValue, String... names) {
        JsonNode value = node(parent, names);
        if (value.isMissingNode() || value.isNull()) {
            return defaultValue;
        }
        if (value.isBoolean()) {
            return value.booleanValue();
        }
        if (value.isTextual()) {
            String text = value.asText().trim();
            if (text.isEmpty()) {
                return defaultValue;
            }
            return Boolean.parseBoolean(text);
        }
        return defaultValue;
    }

    private static int intOrDefault(JsonNode parent, int defaultValue, String... names) {
        JsonNode value = node(parent, names);
        if (value.isMissingNode() || value.isNull()) {
            return defaultValue;
        }
        if (value.isIntegralNumber()) {
            return value.intValue();
        }
        if (value.isTextual()) {
            String text = value.asText().trim();
            if (text.isEmpty()) {
                return defaultValue;
            }
            return Integer.parseInt(text);
        }
        throw new IllegalArgumentException("Expected integer value for: " + List.of(names));
    }

    private static long longOrDefault(JsonNode parent, long defaultValue, String... names) {
        JsonNode value = node(parent, names);
        if (value.isMissingNode() || value.isNull()) {
            return defaultValue;
        }
        if (value.isIntegralNumber()) {
            return value.longValue();
        }
        if (value.isTextual()) {
            String text = value.asText().trim();
            if (text.isEmpty()) {
                return defaultValue;
            }
            return Long.parseLong(text);
        }
        throw new IllegalArgumentException("Expected long value for: " + List.of(names));
    }

    private static List<String> stringList(JsonNode parent, String... names) {
        JsonNode value = node(parent, names);
        if (value.isMissingNode() || value.isNull()) {
            return List.of();
        }
        if (value.isArray()) {
            List<String> items = new ArrayList<>(value.size());
            for (JsonNode item : value) {
                if (!item.isNull()) {
                    String text = item.asText("").trim();
                    if (!text.isEmpty()) {
                        items.add(text);
                    }
                }
            }
            return items;
        }
        if (value.isTextual()) {
            String raw = value.asText().trim();
            if (raw.isEmpty()) {
                return List.of();
            }
            String[] split = raw.split(",");
            List<String> items = new ArrayList<>(split.length);
            for (String part : split) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    items.add(trimmed);
                }
            }
            return items;
        }
        return List.of();
    }

    private static Set<String> normalizeMsgTypes(List<String> values) {
        LinkedHashSet<String> set = new LinkedHashSet<>();
        for (String value : values) {
            if (value != null) {
                String trimmed = value.trim();
                if (!trimmed.isEmpty()) {
                    set.add(trimmed);
                }
            }
        }
        return set;
    }

    private static Path optionalResolvedPath(Path base, String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            return null;
        }
        return resolvePath(base, rawPath);
    }

    private static Path resolvePath(Path base, String rawPath) {
        Path path = Path.of(rawPath);
        if (path.isAbsolute()) {
            return path.normalize();
        }
        return base.resolve(path).normalize();
    }

    public record SessionEndpoint(String listenHost, int listenPort, String localCompId, String remoteCompId) {
        public SessionEndpoint {
            listenHost = Objects.requireNonNull(listenHost, "listenHost");
            localCompId = Objects.requireNonNull(localCompId, "localCompId");
            remoteCompId = Objects.requireNonNull(remoteCompId, "remoteCompId");
        }
    }

    public record Routing(
        Set<String> enabledMsgTypes,
        boolean dropAdminMessages,
        long artificialDelayMs,
        boolean failIfExitNotLoggedOn,
        int maxQueueDepth
    ) {
        public Routing {
            enabledMsgTypes = Set.copyOf(Objects.requireNonNull(enabledMsgTypes, "enabledMsgTypes"));
        }
    }

    public record Mutation(boolean enabled, boolean strictMode, JsonNode rulesInline, Path rulesFile) {
        public Mutation {
            rulesInline = rulesInline == null ? NullNode.instance : rulesInline;
        }

        public RuleSource ruleSource() {
            if (!rulesInline.isMissingNode() && !rulesInline.isNull()) {
                return RuleSource.INLINE;
            }
            if (rulesFile != null) {
                return RuleSource.FILE;
            }
            return RuleSource.NONE;
        }
    }

    public enum RuleSource {
        NONE,
        INLINE,
        FILE
    }

    public record Observability(boolean logInboundOutbound, boolean logFixPayloads) {
    }

    public record Shutdown(long gracefulTimeoutMs) {
    }

    public record StorageDirs(Path workDir, Path aeronDir, Path logDir, boolean cleanupOnStop) {
        public StorageDirs {
            workDir = Objects.requireNonNull(workDir, "workDir").toAbsolutePath().normalize();
            aeronDir = Objects.requireNonNull(aeronDir, "aeronDir").toAbsolutePath().normalize();
            logDir = Objects.requireNonNull(logDir, "logDir").toAbsolutePath().normalize();
        }
    }

    public record Performance(int inboundFragmentLimit, int outboundFragmentLimit, String idleStrategy) {
        public Performance {
            idleStrategy = Objects.requireNonNull(idleStrategy, "idleStrategy");
        }
    }
}
