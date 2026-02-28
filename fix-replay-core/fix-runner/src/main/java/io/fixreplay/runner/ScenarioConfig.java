package io.fixreplay.runner;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fixreplay.compare.CompareConfig;
import io.fixreplay.linker.LinkerConfig;
import io.fixreplay.model.MsgTypeFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ScenarioConfig {
    private final Path inputFolder;
    private final Path expectedFolder;
    private final Path actualFolder;
    private final MsgTypeFilter msgTypeFilter;
    private final LinkerConfig linkerConfig;
    private final CompareConfig compareConfig;
    private final List<SessionMappingRule> sessionMappingRules;
    private final Sessions sessions;
    private final Simulator simulator;
    private final Path cacheFolder;
    private final Path scenarioBaseDirectory;

    private ScenarioConfig(
        Path inputFolder,
        Path expectedFolder,
        Path actualFolder,
        MsgTypeFilter msgTypeFilter,
        LinkerConfig linkerConfig,
        CompareConfig compareConfig,
        List<SessionMappingRule> sessionMappingRules,
        Sessions sessions,
        Simulator simulator,
        Path cacheFolder,
        Path scenarioBaseDirectory
    ) {
        this.inputFolder = inputFolder;
        this.expectedFolder = expectedFolder;
        this.actualFolder = actualFolder;
        this.msgTypeFilter = Objects.requireNonNull(msgTypeFilter, "msgTypeFilter");
        this.linkerConfig = Objects.requireNonNull(linkerConfig, "linkerConfig");
        this.compareConfig = Objects.requireNonNull(compareConfig, "compareConfig");
        this.sessionMappingRules = List.copyOf(Objects.requireNonNull(sessionMappingRules, "sessionMappingRules"));
        this.sessions = Objects.requireNonNull(sessions, "sessions");
        this.simulator = Objects.requireNonNull(simulator, "simulator");
        this.cacheFolder = cacheFolder;
        this.scenarioBaseDirectory = Objects.requireNonNull(scenarioBaseDirectory, "scenarioBaseDirectory");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static ScenarioConfig load(Path configPath) throws IOException {
        ObjectMapper mapper = objectMapperFor(configPath);
        ScenarioSpec spec = mapper.readValue(configPath.toFile(), ScenarioSpec.class);

        Path base = configPath.toAbsolutePath().normalize().getParent();
        if (base == null) {
            base = Path.of(".").toAbsolutePath().normalize();
        }
        Builder builder = builder().scenarioBaseDirectory(base);

        if (spec.inputFolder != null && !spec.inputFolder.isBlank()) {
            builder.inputFolder(resolvePath(base, spec.inputFolder));
        }
        if (spec.expectedFolder != null && !spec.expectedFolder.isBlank()) {
            builder.expectedFolder(resolvePath(base, spec.expectedFolder));
        }

        if (spec.actualFolder != null && !spec.actualFolder.isBlank()) {
            builder.actualFolder(resolvePath(base, spec.actualFolder));
        }
        if (spec.cacheFolder != null && !spec.cacheFolder.isBlank()) {
            builder.cacheFolder(resolvePath(base, spec.cacheFolder));
        }

        if (spec.msgTypeFilter != null && !spec.msgTypeFilter.isEmpty()) {
            builder.msgTypeFilter(new MsgTypeFilter(Set.copyOf(spec.msgTypeFilter)));
        }

        if (spec.linker != null) {
            builder.linkerConfig(spec.linker.toConfig());
        }
        if (spec.compare != null) {
            builder.compareConfig(spec.compare.toConfig());
        }
        if (spec.sessionMappingRules != null && !spec.sessionMappingRules.isEmpty()) {
            List<SessionMappingRule> rules = new ArrayList<>();
            for (SessionMappingRuleSpec ruleSpec : spec.sessionMappingRules) {
                rules.add(ruleSpec.toRule());
            }
            builder.sessionMappingRules(rules);
        }
        builder.sessions(toSessions(spec.sessions));
        builder.simulator(toSimulator(spec.simulator, spec.topLevelMutation, base));

        return builder.build();
    }

    public Path inputFolder() {
        return inputFolder;
    }

    public boolean hasInputFolder() {
        return inputFolder != null;
    }

    public Path expectedFolder() {
        return expectedFolder;
    }

    public boolean hasExpectedFolder() {
        return expectedFolder != null;
    }

    public Path actualFolder() {
        return actualFolder;
    }

    public MsgTypeFilter msgTypeFilter() {
        return msgTypeFilter;
    }

    public LinkerConfig linkerConfig() {
        return linkerConfig;
    }

    public CompareConfig compareConfig() {
        return compareConfig;
    }

    public List<SessionMappingRule> sessionMappingRules() {
        return sessionMappingRules;
    }

    public Sessions sessions() {
        return sessions;
    }

    public Simulator simulator() {
        return simulator;
    }

    public Path cacheFolder() {
        return cacheFolder;
    }

    public boolean hasCacheFolder() {
        return cacheFolder != null;
    }

    public Path scenarioBaseDirectory() {
        return scenarioBaseDirectory;
    }

    public boolean hasActualFolder() {
        return actualFolder != null;
    }

    public Map<SessionKey, Path> resolveInputFiles() throws IOException {
        return resolveBySide(inputFolder, Side.IN);
    }

    public Map<SessionKey, Path> resolveExpectedFiles() throws IOException {
        requireConfiguredFolder(expectedFolder, "expectedFolder");
        return resolveBySide(expectedFolder, Side.OUT);
    }

    public Map<SessionKey, Path> resolveActualFiles() throws IOException {
        if (actualFolder == null) {
            return Map.of();
        }
        return resolveBySide(actualFolder, Side.OUT);
    }

    private Map<SessionKey, Path> resolveBySide(Path folder, Side side) throws IOException {
        requireConfiguredFolder(folder, side == Side.IN ? "inputFolder" : "expectedFolder");
        if (!Files.exists(folder)) {
            throw new IOException("Folder does not exist: " + folder);
        }

        List<Path> files;
        try (var paths = Files.list(folder)) {
            files = paths
                .filter(Files::isRegularFile)
                .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                .toList();
        }

        Map<SessionKey, Path> bySession = new LinkedHashMap<>();
        for (Path file : files) {
            SessionFile sessionFile = mapFile(file.getFileName().toString());
            if (sessionFile == null || sessionFile.side() != side) {
                continue;
            }
            Path previous = bySession.putIfAbsent(sessionFile.sessionKey(), file);
            if (previous != null) {
                throw new IllegalStateException(
                    "Duplicate file mapping for session " + sessionFile.sessionKey().id() + ": " + previous + " and " + file
                );
            }
        }
        return Map.copyOf(bySession);
    }

    private SessionFile mapFile(String fileName) {
        for (SessionMappingRule rule : sessionMappingRules) {
            SessionFile mapped = rule.map(fileName);
            if (mapped != null) {
                return mapped;
            }
        }
        return null;
    }

    private static Path resolvePath(Path base, String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            throw new IllegalArgumentException("Required path is missing in scenario config");
        }
        Path path = Path.of(rawPath);
        if (path.isAbsolute()) {
            return path.normalize();
        }
        return base.resolve(path).normalize();
    }

    private static ObjectMapper objectMapperFor(Path configPath) {
        String lower = configPath.getFileName().toString().toLowerCase();
        ObjectMapper mapper;
        if (lower.endsWith(".yaml") || lower.endsWith(".yml")) {
            mapper = new ObjectMapper(new YAMLFactory());
        } else {
            mapper = new ObjectMapper();
        }
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }

    private static void requireConfiguredFolder(Path folder, String fieldName) {
        if (folder == null) {
            throw new IllegalArgumentException("Missing required scenario field: " + fieldName);
        }
    }

    private static Sessions toSessions(SessionsSpec spec) {
        if (spec == null) {
            return Sessions.defaults();
        }
        SessionIdentity entry = spec.entry == null
            ? SessionIdentity.empty()
            : new SessionIdentity(
                trimToNull(spec.entry.senderCompId),
                trimToNull(spec.entry.targetCompId),
                trimToNull(spec.entry.host),
                spec.entry.port
            );
        SessionIdentity exit = spec.exit == null
            ? SessionIdentity.empty()
            : new SessionIdentity(
                trimToNull(spec.exit.senderCompId),
                trimToNull(spec.exit.targetCompId),
                trimToNull(spec.exit.host),
                spec.exit.port
            );
        return new Sessions(entry, exit);
    }

    private static Simulator toSimulator(SimulatorSpec simulatorSpec, MutationSpec fallbackMutationSpec, Path base) {
        if (simulatorSpec == null) {
            return Simulator.disabled();
        }
        SimulatorEndpoint entry = toEndpoint(simulatorSpec.entry);
        SimulatorEndpoint exit = toEndpoint(simulatorSpec.exit);
        SimulatorRouting routing = toRouting(simulatorSpec.routing);

        MutationSpec mutationSpec = simulatorSpec.mutation != null ? simulatorSpec.mutation : fallbackMutationSpec;
        SimulatorMutation mutation = toMutation(mutationSpec, base);
        SimulatorObservability observability = toObservability(simulatorSpec.observability);
        SimulatorShutdown shutdown = toShutdown(simulatorSpec.shutdown);
        SimulatorArtio artio = toArtio(simulatorSpec.artio, base);

        return new Simulator(
            trimToNull(simulatorSpec.provider),
            Boolean.TRUE.equals(simulatorSpec.enabled),
            trimToNull(simulatorSpec.beginString),
            entry,
            exit,
            routing,
            mutation,
            observability,
            shutdown,
            artio
        );
    }

    private static SimulatorEndpoint toEndpoint(SimulatorEndpointSpec spec) {
        if (spec == null) {
            return SimulatorEndpoint.empty();
        }
        return new SimulatorEndpoint(
            trimToNull(spec.listenHost),
            spec.listenPort,
            trimToNull(spec.localCompId),
            trimToNull(spec.remoteCompId)
        );
    }

    private static SimulatorRouting toRouting(SimulatorRoutingSpec spec) {
        if (spec == null) {
            return SimulatorRouting.defaults();
        }
        List<String> enabledMsgTypes = spec.enabledMsgTypes == null ? null : spec.enabledMsgTypes
            .stream()
            .filter(Objects::nonNull)
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .toList();
        return new SimulatorRouting(
            enabledMsgTypes,
            spec.dropAdminMessages,
            spec.artificialDelayMs,
            spec.failIfExitNotLoggedOn,
            spec.maxQueueDepth
        );
    }

    private static SimulatorMutation toMutation(MutationSpec spec, Path base) {
        if (spec == null) {
            return SimulatorMutation.defaults();
        }
        JsonNode rulesInline = spec.rulesInline == null ? NullNode.instance : spec.rulesInline.deepCopy();
        Path rulesFile = resolveOptionalPath(base, spec.rulesFile);
        return new SimulatorMutation(
            Boolean.TRUE.equals(spec.enabled),
            Boolean.TRUE.equals(spec.strictMode),
            rulesInline,
            rulesFile
        );
    }

    private static SimulatorObservability toObservability(SimulatorObservabilitySpec spec) {
        if (spec == null) {
            return SimulatorObservability.defaults();
        }
        return new SimulatorObservability(spec.logInboundOutbound, spec.logFixPayloads);
    }

    private static SimulatorShutdown toShutdown(SimulatorShutdownSpec spec) {
        if (spec == null) {
            return SimulatorShutdown.defaults();
        }
        return new SimulatorShutdown(spec.gracefulTimeoutMs);
    }

    private static SimulatorArtio toArtio(SimulatorArtioSpec spec, Path base) {
        if (spec == null) {
            return SimulatorArtio.defaults();
        }
        Path workDir = resolveOptionalPath(base, spec.workDir);
        Path aeronDir = resolveOptionalPath(base, spec.aeronDir);
        Path logDir = resolveOptionalPath(base, spec.logDir);
        SimulatorArtioPerformance performance = toArtioPerformance(spec.performance);
        return new SimulatorArtio(workDir, aeronDir, logDir, spec.deleteOnStart, spec.deleteOnStop, performance);
    }

    private static SimulatorArtioPerformance toArtioPerformance(SimulatorArtioPerformanceSpec spec) {
        if (spec == null) {
            return SimulatorArtioPerformance.defaults();
        }
        return new SimulatorArtioPerformance(spec.inboundFragmentLimit, spec.outboundFragmentLimit, trimToNull(spec.idleStrategy));
    }

    private static Path resolveOptionalPath(Path base, String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            return null;
        }
        return resolvePath(base, rawPath);
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    public static final class Builder {
        private Path inputFolder;
        private Path expectedFolder;
        private Path actualFolder;
        private MsgTypeFilter msgTypeFilter = new MsgTypeFilter();
        private LinkerConfig linkerConfig = LinkerConfig.defaults();
        private CompareConfig compareConfig = CompareConfig.defaults();
        private List<SessionMappingRule> sessionMappingRules = List.of(SessionMappingRule.defaultRule());
        private Sessions sessions = Sessions.defaults();
        private Simulator simulator = Simulator.disabled();
        private Path cacheFolder;
        private Path scenarioBaseDirectory = Path.of(".").toAbsolutePath().normalize();

        public Builder inputFolder(Path inputFolder) {
            this.inputFolder = inputFolder;
            return this;
        }

        public Builder expectedFolder(Path expectedFolder) {
            this.expectedFolder = expectedFolder;
            return this;
        }

        public Builder actualFolder(Path actualFolder) {
            this.actualFolder = actualFolder;
            return this;
        }

        public Builder msgTypeFilter(MsgTypeFilter msgTypeFilter) {
            this.msgTypeFilter = msgTypeFilter;
            return this;
        }

        public Builder linkerConfig(LinkerConfig linkerConfig) {
            this.linkerConfig = linkerConfig;
            return this;
        }

        public Builder compareConfig(CompareConfig compareConfig) {
            this.compareConfig = compareConfig;
            return this;
        }

        public Builder sessionMappingRules(List<SessionMappingRule> sessionMappingRules) {
            this.sessionMappingRules = List.copyOf(sessionMappingRules);
            return this;
        }

        public Builder sessions(Sessions sessions) {
            this.sessions = Objects.requireNonNull(sessions, "sessions");
            return this;
        }

        public Builder simulator(Simulator simulator) {
            this.simulator = Objects.requireNonNull(simulator, "simulator");
            return this;
        }

        public Builder cacheFolder(Path cacheFolder) {
            this.cacheFolder = cacheFolder;
            return this;
        }

        public Builder scenarioBaseDirectory(Path scenarioBaseDirectory) {
            this.scenarioBaseDirectory = Objects.requireNonNull(scenarioBaseDirectory, "scenarioBaseDirectory");
            return this;
        }

        public ScenarioConfig build() {
            return new ScenarioConfig(
                inputFolder,
                expectedFolder,
                actualFolder,
                msgTypeFilter,
                linkerConfig,
                compareConfig,
                sessionMappingRules,
                sessions,
                simulator,
                cacheFolder,
                scenarioBaseDirectory
            );
        }
    }

    public enum Side {
        IN,
        OUT;

        static Side fromToken(String token) {
            if ("in".equalsIgnoreCase(token)) {
                return IN;
            }
            if ("out".equalsIgnoreCase(token)) {
                return OUT;
            }
            return null;
        }
    }

    public record SessionFile(SessionKey sessionKey, Side side) {
    }

    public record Sessions(SessionIdentity entry, SessionIdentity exit) {
        public Sessions {
            entry = entry == null ? SessionIdentity.empty() : entry;
            exit = exit == null ? SessionIdentity.empty() : exit;
        }

        static Sessions defaults() {
            return new Sessions(SessionIdentity.empty(), SessionIdentity.empty());
        }
    }

    public record SessionIdentity(String senderCompId, String targetCompId, String host, Integer port) {
        static SessionIdentity empty() {
            return new SessionIdentity(null, null, null, null);
        }
    }

    public record Simulator(
        String provider,
        boolean enabled,
        String beginString,
        SimulatorEndpoint entry,
        SimulatorEndpoint exit,
        SimulatorRouting routing,
        SimulatorMutation mutation,
        SimulatorObservability observability,
        SimulatorShutdown shutdown,
        SimulatorArtio artio
    ) {
        public Simulator {
            entry = entry == null ? SimulatorEndpoint.empty() : entry;
            exit = exit == null ? SimulatorEndpoint.empty() : exit;
            routing = routing == null ? SimulatorRouting.defaults() : routing;
            mutation = mutation == null ? SimulatorMutation.defaults() : mutation;
            observability = observability == null ? SimulatorObservability.defaults() : observability;
            shutdown = shutdown == null ? SimulatorShutdown.defaults() : shutdown;
            artio = artio == null ? SimulatorArtio.defaults() : artio;
        }

        static Simulator disabled() {
            return new Simulator(
                "none",
                false,
                null,
                SimulatorEndpoint.empty(),
                SimulatorEndpoint.empty(),
                SimulatorRouting.defaults(),
                SimulatorMutation.defaults(),
                SimulatorObservability.defaults(),
                SimulatorShutdown.defaults(),
                SimulatorArtio.defaults()
            );
        }
    }

    public record SimulatorEndpoint(String listenHost, Integer listenPort, String localCompId, String remoteCompId) {
        static SimulatorEndpoint empty() {
            return new SimulatorEndpoint(null, null, null, null);
        }
    }

    public record SimulatorRouting(
        List<String> enabledMsgTypes,
        Boolean dropAdminMessages,
        Long artificialDelayMs,
        Boolean failIfExitNotLoggedOn,
        Integer maxQueueDepth
    ) {
        public SimulatorRouting {
            enabledMsgTypes = enabledMsgTypes == null ? null : List.copyOf(enabledMsgTypes);
        }

        static SimulatorRouting defaults() {
            return new SimulatorRouting(null, null, null, null, null);
        }
    }

    public record SimulatorMutation(boolean enabled, boolean strictMode, JsonNode rulesInline, Path rulesFile) {
        public SimulatorMutation {
            rulesInline = rulesInline == null ? NullNode.instance : rulesInline.deepCopy();
            rulesFile = rulesFile == null ? null : rulesFile.toAbsolutePath().normalize();
        }

        static SimulatorMutation defaults() {
            return new SimulatorMutation(false, false, NullNode.instance, null);
        }
    }

    public record SimulatorObservability(Boolean logInboundOutbound, Boolean logFixPayloads) {
        static SimulatorObservability defaults() {
            return new SimulatorObservability(null, null);
        }
    }

    public record SimulatorShutdown(Long gracefulTimeoutMs) {
        static SimulatorShutdown defaults() {
            return new SimulatorShutdown(null);
        }
    }

    public record SimulatorArtio(
        Path workDir,
        Path aeronDir,
        Path logDir,
        Boolean deleteOnStart,
        Boolean deleteOnStop,
        SimulatorArtioPerformance performance
    ) {
        public SimulatorArtio {
            performance = performance == null ? SimulatorArtioPerformance.defaults() : performance;
            workDir = workDir == null ? null : workDir.toAbsolutePath().normalize();
            aeronDir = aeronDir == null ? null : aeronDir.toAbsolutePath().normalize();
            logDir = logDir == null ? null : logDir.toAbsolutePath().normalize();
        }

        static SimulatorArtio defaults() {
            return new SimulatorArtio(null, null, null, null, null, SimulatorArtioPerformance.defaults());
        }
    }

    public record SimulatorArtioPerformance(Integer inboundFragmentLimit, Integer outboundFragmentLimit, String idleStrategy) {
        static SimulatorArtioPerformance defaults() {
            return new SimulatorArtioPerformance(null, null, null);
        }
    }

    public record SessionMappingRule(Pattern pattern, String senderGroup, String targetGroup, String sideGroup) {
        public SessionMappingRule {
            Objects.requireNonNull(pattern, "pattern");
            senderGroup = Objects.requireNonNull(senderGroup, "senderGroup");
            targetGroup = Objects.requireNonNull(targetGroup, "targetGroup");
            sideGroup = Objects.requireNonNull(sideGroup, "sideGroup");
        }

        public static SessionMappingRule defaultRule() {
            return new SessionMappingRule(
                Pattern.compile("^(?<sender>[^_]+)_(?<target>[^.]+)\\.(?<side>in|out)$"),
                "sender",
                "target",
                "side"
            );
        }

        public SessionFile map(String fileName) {
            Matcher matcher = pattern.matcher(fileName);
            if (!matcher.matches()) {
                return null;
            }
            Side side = Side.fromToken(matcher.group(sideGroup));
            if (side == null) {
                return null;
            }
            SessionKey key = new SessionKey(matcher.group(senderGroup), matcher.group(targetGroup));
            return new SessionFile(key, side);
        }
    }

    static final class ScenarioSpec {
        @JsonAlias({"input_folder"})
        public String inputFolder;
        @JsonAlias({"expected_folder"})
        public String expectedFolder;
        @JsonAlias({"actual_folder"})
        public String actualFolder;
        @JsonAlias({"cache_folder"})
        public String cacheFolder;
        @JsonAlias({"msg_type_filter"})
        public Set<String> msgTypeFilter;
        public LinkerSpec linker;
        public CompareSpec compare;
        @JsonAlias({"session_mapping_rules"})
        public List<SessionMappingRuleSpec> sessionMappingRules;
        public SessionsSpec sessions;
        public IoSpec io;
        public SimulatorSpec simulator;
        @JsonAlias({"mutation"})
        public MutationSpec topLevelMutation;
    }

    static final class SessionMappingRuleSpec {
        public String regex;
        public String senderGroup = "sender";
        public String targetGroup = "target";
        public String sideGroup = "side";

        SessionMappingRule toRule() {
            return new SessionMappingRule(
                Pattern.compile(Objects.requireNonNull(regex, "sessionMappingRules.regex")),
                Objects.requireNonNull(senderGroup, "sessionMappingRules.senderGroup"),
                Objects.requireNonNull(targetGroup, "sessionMappingRules.targetGroup"),
                Objects.requireNonNull(sideGroup, "sessionMappingRules.sideGroup")
            );
        }
    }

    static final class LinkerSpec {
        public List<Integer> candidateTags;
        public List<List<Integer>> candidateCombinations;
        public Map<String, List<List<Integer>>> msgTypeOverrides;
        public Map<String, NormalizerSpec> normalizers;

        LinkerConfig toConfig() {
            LinkerConfig.Builder builder = LinkerConfig.builder();
            if (candidateTags != null && !candidateTags.isEmpty()) {
                builder.candidateTags(candidateTags);
            }
            if (candidateCombinations != null && !candidateCombinations.isEmpty()) {
                builder.candidateCombinations(candidateCombinations);
            }
            if (msgTypeOverrides != null) {
                for (Map.Entry<String, List<List<Integer>>> entry : msgTypeOverrides.entrySet()) {
                    builder.overrideCandidates(entry.getKey(), entry.getValue());
                }
            }
            if (normalizers != null) {
                for (Map.Entry<String, NormalizerSpec> entry : normalizers.entrySet()) {
                    builder.normalizer(Integer.parseInt(entry.getKey()), entry.getValue().toLinkerNormalizer());
                }
            }
            return builder.build();
        }
    }

    static final class CompareSpec {
        public Set<Integer> defaultIncludeTags;
        public Set<Integer> defaultExcludeTags;
        public Boolean excludeTimeLikeTags;
        public Map<String, Set<Integer>> includeTagsByMsgType;
        public Map<String, Set<Integer>> excludeTagsByMsgType;
        public Map<String, NormalizerSpec> normalizers;

        CompareConfig toConfig() {
            CompareConfig.Builder builder = CompareConfig.builder();
            if (defaultIncludeTags != null && !defaultIncludeTags.isEmpty()) {
                builder.defaultIncludeTags(defaultIncludeTags);
            }
            if (defaultExcludeTags != null && !defaultExcludeTags.isEmpty()) {
                builder.defaultExcludeTags(defaultExcludeTags);
            }
            if (excludeTimeLikeTags != null) {
                builder.excludeTimeLikeTags(excludeTimeLikeTags);
            }
            if (includeTagsByMsgType != null) {
                for (Map.Entry<String, Set<Integer>> entry : includeTagsByMsgType.entrySet()) {
                    builder.includeTagsForMsgType(entry.getKey(), entry.getValue());
                }
            }
            if (excludeTagsByMsgType != null) {
                for (Map.Entry<String, Set<Integer>> entry : excludeTagsByMsgType.entrySet()) {
                    builder.excludeTagsForMsgType(entry.getKey(), entry.getValue());
                }
            }
            if (normalizers != null) {
                for (Map.Entry<String, NormalizerSpec> entry : normalizers.entrySet()) {
                    builder.normalizer(Integer.parseInt(entry.getKey()), entry.getValue().toCompareNormalizer());
                }
            }
            return builder.build();
        }
    }

    static final class NormalizerSpec {
        public Boolean trim;
        public List<RegexReplacement> regexReplacements;

        LinkerConfig.TagNormalizer toLinkerNormalizer() {
            LinkerConfig.TagNormalizer.Builder builder = LinkerConfig.TagNormalizer.builder();
            if (trim != null) {
                builder.trim(trim);
            }
            if (regexReplacements != null) {
                for (RegexReplacement replacement : regexReplacements) {
                    builder.regexReplace(replacement.pattern, replacement.replacement);
                }
            }
            return builder.build();
        }

        CompareConfig.TagNormalizer toCompareNormalizer() {
            CompareConfig.TagNormalizer.Builder builder = CompareConfig.TagNormalizer.builder();
            if (trim != null) {
                builder.trim(trim);
            }
            if (regexReplacements != null) {
                for (RegexReplacement replacement : regexReplacements) {
                    builder.regexReplace(replacement.pattern, replacement.replacement);
                }
            }
            return builder.build();
        }
    }

    static final class RegexReplacement {
        public String pattern;
        public String replacement;
    }

    static final class IoSpec {
        public String mode;
    }

    static final class SessionsSpec {
        public SessionIdentitySpec entry;
        public SessionIdentitySpec exit;
    }

    static final class SessionIdentitySpec {
        @JsonAlias({"sender_comp_id"})
        public String senderCompId;

        @JsonAlias({"target_comp_id"})
        public String targetCompId;

        @JsonAlias({"connect_host", "initiator_host", "remote_host", "host"})
        public String host;

        @JsonAlias({"connect_port", "initiator_port", "remote_port", "port"})
        public Integer port;
    }

    static final class SimulatorSpec {
        public String provider;
        public Boolean enabled;

        @JsonAlias({"begin_string"})
        public String beginString;

        public SimulatorEndpointSpec entry;
        public SimulatorEndpointSpec exit;
        public SimulatorRoutingSpec routing;
        public MutationSpec mutation;
        public SimulatorObservabilitySpec observability;
        public SimulatorShutdownSpec shutdown;
        public SimulatorArtioSpec artio;
    }

    static final class SimulatorEndpointSpec {
        @JsonAlias({"listen_host", "host"})
        public String listenHost;

        @JsonAlias({"listen_port", "port"})
        public Integer listenPort;

        @JsonAlias({"local_comp_id", "sender_comp_id"})
        public String localCompId;

        @JsonAlias({"remote_comp_id", "target_comp_id"})
        public String remoteCompId;
    }

    static final class SimulatorRoutingSpec {
        @JsonAlias({"enabled_msg_types"})
        public List<String> enabledMsgTypes;

        @JsonAlias({"drop_admin_messages"})
        public Boolean dropAdminMessages;

        @JsonAlias({"artificial_delay_ms"})
        public Long artificialDelayMs;

        @JsonAlias({"fail_if_exit_not_logged_on"})
        public Boolean failIfExitNotLoggedOn;

        @JsonAlias({"max_queue_depth"})
        public Integer maxQueueDepth;
    }

    static final class MutationSpec {
        public Boolean enabled;

        @JsonAlias({"strict_mode"})
        public Boolean strictMode;

        @JsonAlias({"rules_inline"})
        public JsonNode rulesInline;

        @JsonAlias({"rules_file"})
        public String rulesFile;
    }

    static final class SimulatorObservabilitySpec {
        @JsonAlias({"log_inbound_outbound"})
        public Boolean logInboundOutbound;

        @JsonAlias({"log_fix_payloads"})
        public Boolean logFixPayloads;
    }

    static final class SimulatorShutdownSpec {
        @JsonAlias({"graceful_timeout_ms"})
        public Long gracefulTimeoutMs;
    }

    static final class SimulatorArtioSpec {
        @JsonAlias({"work_dir"})
        public String workDir;

        @JsonAlias({"aeron_dir"})
        public String aeronDir;

        @JsonAlias({"log_dir"})
        public String logDir;

        @JsonAlias({"delete_on_start"})
        public Boolean deleteOnStart;

        @JsonAlias({"delete_on_stop", "cleanup_on_stop", "cleanup_work_dirs_on_stop"})
        public Boolean deleteOnStop;

        public SimulatorArtioPerformanceSpec performance;
    }

    static final class SimulatorArtioPerformanceSpec {
        @JsonAlias({"inbound_fragment_limit"})
        public Integer inboundFragmentLimit;

        @JsonAlias({"outbound_fragment_limit"})
        public Integer outboundFragmentLimit;

        @JsonAlias({"idle_strategy"})
        public String idleStrategy;
    }
}
