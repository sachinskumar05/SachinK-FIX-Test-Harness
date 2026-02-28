package io.fixreplay.runner;

import com.fasterxml.jackson.databind.ObjectMapper;
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

    private ScenarioConfig(
        Path inputFolder,
        Path expectedFolder,
        Path actualFolder,
        MsgTypeFilter msgTypeFilter,
        LinkerConfig linkerConfig,
        CompareConfig compareConfig,
        List<SessionMappingRule> sessionMappingRules
    ) {
        this.inputFolder = Objects.requireNonNull(inputFolder, "inputFolder");
        this.expectedFolder = Objects.requireNonNull(expectedFolder, "expectedFolder");
        this.actualFolder = actualFolder;
        this.msgTypeFilter = Objects.requireNonNull(msgTypeFilter, "msgTypeFilter");
        this.linkerConfig = Objects.requireNonNull(linkerConfig, "linkerConfig");
        this.compareConfig = Objects.requireNonNull(compareConfig, "compareConfig");
        this.sessionMappingRules = List.copyOf(Objects.requireNonNull(sessionMappingRules, "sessionMappingRules"));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static ScenarioConfig load(Path configPath) throws IOException {
        ObjectMapper mapper = objectMapperFor(configPath);
        ScenarioSpec spec = mapper.readValue(configPath.toFile(), ScenarioSpec.class);

        Path base = configPath.toAbsolutePath().getParent();
        Builder builder = builder()
            .inputFolder(resolvePath(base, spec.inputFolder))
            .expectedFolder(resolvePath(base, spec.expectedFolder));

        if (spec.actualFolder != null && !spec.actualFolder.isBlank()) {
            builder.actualFolder(resolvePath(base, spec.actualFolder));
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

        return builder.build();
    }

    public Path inputFolder() {
        return inputFolder;
    }

    public Path expectedFolder() {
        return expectedFolder;
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

    public boolean hasActualFolder() {
        return actualFolder != null;
    }

    public Map<SessionKey, Path> resolveInputFiles() throws IOException {
        return resolveBySide(inputFolder, Side.IN);
    }

    public Map<SessionKey, Path> resolveExpectedFiles() throws IOException {
        return resolveBySide(expectedFolder, Side.OUT);
    }

    public Map<SessionKey, Path> resolveActualFiles() throws IOException {
        if (actualFolder == null) {
            return Map.of();
        }
        return resolveBySide(actualFolder, Side.OUT);
    }

    private Map<SessionKey, Path> resolveBySide(Path folder, Side side) throws IOException {
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
        if (lower.endsWith(".yaml") || lower.endsWith(".yml")) {
            return new ObjectMapper(new YAMLFactory());
        }
        return new ObjectMapper();
    }

    public static final class Builder {
        private Path inputFolder;
        private Path expectedFolder;
        private Path actualFolder;
        private MsgTypeFilter msgTypeFilter = new MsgTypeFilter();
        private LinkerConfig linkerConfig = LinkerConfig.defaults();
        private CompareConfig compareConfig = CompareConfig.defaults();
        private List<SessionMappingRule> sessionMappingRules = List.of(SessionMappingRule.defaultRule());

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

        public ScenarioConfig build() {
            return new ScenarioConfig(
                inputFolder,
                expectedFolder,
                actualFolder,
                msgTypeFilter,
                linkerConfig,
                compareConfig,
                sessionMappingRules
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
        public String inputFolder;
        public String expectedFolder;
        public String actualFolder;
        public Set<String> msgTypeFilter;
        public LinkerSpec linker;
        public CompareSpec compare;
        public List<SessionMappingRuleSpec> sessionMappingRules;
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
}
