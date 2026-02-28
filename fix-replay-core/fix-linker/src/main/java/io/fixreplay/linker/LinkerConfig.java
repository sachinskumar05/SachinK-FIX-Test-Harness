package io.fixreplay.linker;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

public final class LinkerConfig {
    private static final List<Integer> DEFAULT_CANDIDATE_TAGS = List.of(11, 41, 37, 17, 55, 54, 60);

    private final List<Integer> candidateTags;
    private final List<List<Integer>> candidateCombinations;
    private final Map<String, List<List<Integer>>> perMsgTypeCandidates;
    private final Map<Integer, TagNormalizer> normalizersByTag;

    private LinkerConfig(
        List<Integer> candidateTags,
        List<List<Integer>> candidateCombinations,
        Map<String, List<List<Integer>>> perMsgTypeCandidates,
        Map<Integer, TagNormalizer> normalizersByTag
    ) {
        this.candidateTags = List.copyOf(candidateTags);
        this.candidateCombinations = normalizeCombinations(candidateCombinations);
        this.perMsgTypeCandidates = normalizeOverrides(perMsgTypeCandidates);
        this.normalizersByTag = Map.copyOf(normalizersByTag);
    }

    public static LinkerConfig defaults() {
        List<List<Integer>> combinations = generateCombinations(DEFAULT_CANDIDATE_TAGS, 1, 2);
        return new LinkerConfig(
            DEFAULT_CANDIDATE_TAGS,
            combinations,
            Map.of(),
            Map.of()
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<Integer> candidateTags() {
        return candidateTags;
    }

    public List<List<Integer>> candidateCombinations() {
        return candidateCombinations;
    }

    public List<List<Integer>> candidatesFor(String msgType) {
        List<List<Integer>> override = perMsgTypeCandidates.get(msgType);
        return override == null || override.isEmpty() ? candidateCombinations : override;
    }

    public String normalizeValue(int tag, String value) {
        if (value == null) {
            return null;
        }
        TagNormalizer normalizer = normalizersByTag.get(tag);
        return normalizer == null ? value : normalizer.apply(value);
    }

    private static List<List<Integer>> generateCombinations(List<Integer> tags, int minSize, int maxSize) {
        List<List<Integer>> combinations = new ArrayList<>();
        if (minSize <= 1 && maxSize >= 1) {
            for (Integer tag : tags) {
                combinations.add(List.of(tag));
            }
        }
        if (minSize <= 2 && maxSize >= 2) {
            for (int i = 0; i < tags.size(); i++) {
                for (int j = i + 1; j < tags.size(); j++) {
                    int first = Math.min(tags.get(i), tags.get(j));
                    int second = Math.max(tags.get(i), tags.get(j));
                    combinations.add(List.of(first, second));
                }
            }
        }
        return normalizeCombinations(combinations);
    }

    private static List<List<Integer>> normalizeCombinations(List<List<Integer>> combinations) {
        Set<List<Integer>> ordered = new LinkedHashSet<>();
        for (List<Integer> combination : combinations) {
            if (combination == null || combination.isEmpty()) {
                continue;
            }
            List<Integer> copy = new ArrayList<>(combination.size());
            for (Integer tag : combination) {
                if (tag == null || tag <= 0) {
                    throw new IllegalArgumentException("Candidate tag must be positive: " + tag);
                }
                copy.add(tag);
            }
            copy.sort(Integer::compareTo);
            ordered.add(List.copyOf(copy));
        }
        return List.copyOf(ordered);
    }

    private static Map<String, List<List<Integer>>> normalizeOverrides(Map<String, List<List<Integer>>> overrides) {
        Map<String, List<List<Integer>>> out = new LinkedHashMap<>();
        for (Map.Entry<String, List<List<Integer>>> entry : overrides.entrySet()) {
            String msgType = Objects.requireNonNull(entry.getKey(), "msgType override key");
            out.put(msgType, normalizeCombinations(entry.getValue()));
        }
        return Map.copyOf(out);
    }

    public static final class Builder {
        private List<Integer> candidateTags = DEFAULT_CANDIDATE_TAGS;
        private List<List<Integer>> candidateCombinations;
        private final Map<String, List<List<Integer>>> overrides = new LinkedHashMap<>();
        private final Map<Integer, TagNormalizer> normalizers = new LinkedHashMap<>();

        public Builder candidateTags(List<Integer> tags) {
            this.candidateTags = List.copyOf(tags);
            return this;
        }

        public Builder candidateCombinations(List<List<Integer>> combinations) {
            this.candidateCombinations = List.copyOf(combinations);
            return this;
        }

        public Builder overrideCandidates(String msgType, List<List<Integer>> combinations) {
            overrides.put(msgType, List.copyOf(combinations));
            return this;
        }

        public Builder normalizer(int tag, TagNormalizer normalizer) {
            normalizers.put(tag, normalizer);
            return this;
        }

        public LinkerConfig build() {
            List<List<Integer>> combinations = candidateCombinations;
            if (combinations == null) {
                combinations = generateCombinations(candidateTags, 1, 2);
            }
            return new LinkerConfig(candidateTags, combinations, overrides, normalizers);
        }
    }

    public static final class TagNormalizer {
        private final boolean trim;
        private final List<RegexRule> regexRules;

        private TagNormalizer(boolean trim, List<RegexRule> regexRules) {
            this.trim = trim;
            this.regexRules = List.copyOf(regexRules);
        }

        public static TagNormalizer none() {
            return new TagNormalizer(false, List.of());
        }

        public static Builder builder() {
            return new Builder();
        }

        public String apply(String rawValue) {
            String value = rawValue;
            if (trim) {
                value = value.trim();
            }
            for (RegexRule rule : regexRules) {
                value = rule.pattern.matcher(value).replaceAll(rule.replacement);
            }
            return value;
        }

        public static final class Builder {
            private boolean trim;
            private final List<RegexRule> regexRules = new ArrayList<>();

            public Builder trim(boolean trim) {
                this.trim = trim;
                return this;
            }

            public Builder regexReplace(String pattern, String replacement) {
                regexRules.add(new RegexRule(Pattern.compile(pattern), replacement));
                return this;
            }

            public TagNormalizer build() {
                return new TagNormalizer(trim, regexRules);
            }
        }
    }

    private record RegexRule(Pattern pattern, String replacement) {
    }
}
