package io.fixreplay.simulator.artio;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import io.fixreplay.runner.ScenarioConfig;
import java.io.IOException;
import java.nio.file.Path;
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
    private static final boolean DEFAULT_DELETE_ON_START = true;
    private static final boolean DEFAULT_DELETE_ON_STOP = false;

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
        return fromScenario(ScenarioConfig.load(scenarioPath));
    }

    public static ArtioSimulatorConfig fromScenario(ScenarioConfig scenario) {
        Objects.requireNonNull(scenario, "scenario");

        ScenarioConfig.Simulator simulator = scenario.simulator();
        ScenarioConfig.Sessions sessions = scenario.sessions();

        String provider = firstNonBlank(simulator.provider(), DEFAULT_PROVIDER).toLowerCase(Locale.ROOT);
        boolean enabled = simulator.enabled();
        String beginString = firstNonBlank(simulator.beginString(), DEFAULT_BEGIN_STRING);

        String defaultEntryLocalCompId = firstNonBlank(sessions.entry().targetCompId(), DEFAULT_LOCAL_COMP_ID);
        String defaultEntryRemoteCompId = firstNonBlank(sessions.entry().senderCompId(), DEFAULT_ENTRY_REMOTE_COMP_ID);
        String defaultExitLocalCompId = firstNonBlank(sessions.exit().senderCompId(), DEFAULT_LOCAL_COMP_ID);
        String defaultExitRemoteCompId = firstNonBlank(sessions.exit().targetCompId(), DEFAULT_EXIT_REMOTE_COMP_ID);

        ScenarioConfig.SimulatorEndpoint entrySpec = simulator.entry();
        ScenarioConfig.SimulatorEndpoint exitSpec = simulator.exit();

        SessionEndpoint entry = new SessionEndpoint(
            firstNonBlank(entrySpec.listenHost(), DEFAULT_HOST),
            intOrDefault(entrySpec.listenPort(), 0),
            firstNonBlank(entrySpec.localCompId(), defaultEntryLocalCompId),
            firstNonBlank(entrySpec.remoteCompId(), defaultEntryRemoteCompId)
        );
        SessionEndpoint exit = new SessionEndpoint(
            firstNonBlank(exitSpec.listenHost(), DEFAULT_HOST),
            intOrDefault(exitSpec.listenPort(), 0),
            firstNonBlank(exitSpec.localCompId(), defaultExitLocalCompId),
            firstNonBlank(exitSpec.remoteCompId(), defaultExitRemoteCompId)
        );

        ScenarioConfig.SimulatorRouting routingSpec = simulator.routing();
        Set<String> enabledMsgTypes = normalizeMsgTypes(routingSpec.enabledMsgTypes());
        if (enabledMsgTypes.isEmpty()) {
            enabledMsgTypes = new LinkedHashSet<>(DEFAULT_ENABLED_MSG_TYPES);
        }
        Routing routing = new Routing(
            Set.copyOf(enabledMsgTypes),
            booleanOrDefault(routingSpec.dropAdminMessages(), true),
            longOrDefault(routingSpec.artificialDelayMs(), 0L),
            booleanOrDefault(routingSpec.failIfExitNotLoggedOn(), true),
            intOrDefault(routingSpec.maxQueueDepth(), DEFAULT_MAX_QUEUE_DEPTH)
        );

        ScenarioConfig.SimulatorMutation mutationSpec = simulator.mutation();
        JsonNode inlineRules = copyNode(mutationSpec.rulesInline());
        Mutation mutation = new Mutation(
            mutationSpec.enabled(),
            mutationSpec.strictMode(),
            inlineRules,
            mutationSpec.rulesFile()
        );

        ScenarioConfig.SimulatorObservability observabilitySpec = simulator.observability();
        Observability observability = new Observability(
            booleanOrDefault(observabilitySpec.logInboundOutbound(), true),
            booleanOrDefault(observabilitySpec.logFixPayloads(), false)
        );

        ScenarioConfig.SimulatorShutdown shutdownSpec = simulator.shutdown();
        Shutdown shutdown = new Shutdown(longOrDefault(shutdownSpec.gracefulTimeoutMs(), DEFAULT_GRACEFUL_TIMEOUT_MS));

        ScenarioConfig.SimulatorArtio artioSpec = simulator.artio();
        Path defaultWorkDir = defaultWorkDir(scenario);
        Path workDir = pathOrDefault(artioSpec.workDir(), defaultWorkDir);
        Path aeronDir = pathOrDefault(artioSpec.aeronDir(), workDir.resolve("aeron"));
        Path logDir = pathOrDefault(artioSpec.logDir(), workDir.resolve("logs"));
        StorageDirs storageDirs = new StorageDirs(
            workDir,
            aeronDir,
            logDir,
            booleanOrDefault(artioSpec.deleteOnStart(), DEFAULT_DELETE_ON_START),
            booleanOrDefault(artioSpec.deleteOnStop(), DEFAULT_DELETE_ON_STOP)
        );

        ScenarioConfig.SimulatorArtioPerformance performanceSpec = artioSpec.performance();
        Performance performance = new Performance(
            intOrDefault(performanceSpec.inboundFragmentLimit(), DEFAULT_FRAGMENT_LIMIT),
            intOrDefault(performanceSpec.outboundFragmentLimit(), DEFAULT_FRAGMENT_LIMIT),
            firstNonBlank(performanceSpec.idleStrategy(), DEFAULT_IDLE_STRATEGY)
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
        return "artio".equalsIgnoreCase(provider) || "none".equalsIgnoreCase(provider);
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

        if (!"artio".equalsIgnoreCase(provider)) {
            throw new IllegalArgumentException("simulator.provider must be 'artio' when simulator.enabled=true");
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

    private static Path defaultWorkDir(ScenarioConfig scenario) {
        if (scenario.hasCacheFolder()) {
            return scenario.cacheFolder();
        }
        return scenario.scenarioBaseDirectory().resolve(".simulator-artio").normalize();
    }

    private static boolean booleanOrDefault(Boolean value, boolean defaultValue) {
        return value == null ? defaultValue : value;
    }

    private static int intOrDefault(Integer value, int defaultValue) {
        return value == null ? defaultValue : value;
    }

    private static long longOrDefault(Long value, long defaultValue) {
        return value == null ? defaultValue : value;
    }

    private static Path pathOrDefault(Path value, Path defaultValue) {
        if (value == null) {
            return defaultValue.toAbsolutePath().normalize();
        }
        return value.toAbsolutePath().normalize();
    }

    private static String firstNonBlank(String... candidates) {
        for (String candidate : candidates) {
            if (candidate != null && !candidate.isBlank()) {
                return candidate;
            }
        }
        return null;
    }

    private static JsonNode copyNode(JsonNode value) {
        if (value == null || value.isMissingNode() || value.isNull()) {
            return NullNode.instance;
        }
        return value.deepCopy();
    }

    private static Set<String> normalizeMsgTypes(List<String> values) {
        if (values == null || values.isEmpty()) {
            return Set.of();
        }
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
            if (hasInlineRules()) {
                return RuleSource.INLINE;
            }
            if (rulesFile != null) {
                return RuleSource.FILE;
            }
            return RuleSource.NONE;
        }

        private boolean hasInlineRules() {
            if (rulesInline.isMissingNode() || rulesInline.isNull()) {
                return false;
            }
            JsonNode ruleArray = rulesInline.path("rules");
            if (ruleArray.isArray()) {
                return ruleArray.size() > 0;
            }
            if (rulesInline.isArray()) {
                return rulesInline.size() > 0;
            }
            return !rulesInline.isEmpty();
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

    public record StorageDirs(
        Path workDir,
        Path aeronDir,
        Path logDir,
        boolean deleteOnStart,
        boolean deleteOnStop
    ) {
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
