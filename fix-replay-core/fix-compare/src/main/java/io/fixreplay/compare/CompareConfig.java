package io.fixreplay.compare;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

public final class CompareConfig {
    private static final Set<Integer> DEFAULT_EXCLUDE_TAGS = Set.of(8, 9, 10, 34, 52, 122);
    private static final Set<Integer> TIME_LIKE_TAGS = Set.of(60);

    private final Set<Integer> defaultIncludeTags;
    private final Set<Integer> defaultExcludeTags;
    private final Map<String, Set<Integer>> includeTagsByMsgType;
    private final Map<String, Set<Integer>> excludeTagsByMsgType;
    private final boolean excludeTimeLikeTags;
    private final Map<Integer, TagNormalizer> normalizersByTag;

    private CompareConfig(
        Set<Integer> defaultIncludeTags,
        Set<Integer> defaultExcludeTags,
        Map<String, Set<Integer>> includeTagsByMsgType,
        Map<String, Set<Integer>> excludeTagsByMsgType,
        boolean excludeTimeLikeTags,
        Map<Integer, TagNormalizer> normalizersByTag
    ) {
        this.defaultIncludeTags = defaultIncludeTags == null ? null : Set.copyOf(defaultIncludeTags);
        this.defaultExcludeTags = Set.copyOf(defaultExcludeTags);
        this.includeTagsByMsgType = freezeTagMap(includeTagsByMsgType);
        this.excludeTagsByMsgType = freezeTagMap(excludeTagsByMsgType);
        this.excludeTimeLikeTags = excludeTimeLikeTags;
        this.normalizersByTag = Map.copyOf(normalizersByTag);
    }

    public static CompareConfig defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public Set<Integer> tagsToCompare(String msgType, Set<Integer> expectedTags, Set<Integer> actualTags) {
        Set<Integer> combined = new TreeSet<>();
        combined.addAll(expectedTags);
        combined.addAll(actualTags);

        Set<Integer> include = includeTagsByMsgType.get(msgType);
        if (include == null) {
            include = defaultIncludeTags;
        }
        if (include != null && !include.isEmpty()) {
            combined.retainAll(include);
        }

        Set<Integer> exclude = new TreeSet<>(defaultExcludeTags);
        if (excludeTimeLikeTags) {
            exclude.addAll(TIME_LIKE_TAGS);
        }
        Set<Integer> msgTypeExcludes = excludeTagsByMsgType.get(msgType);
        if (msgTypeExcludes != null) {
            exclude.addAll(msgTypeExcludes);
        }
        combined.removeAll(exclude);
        return Set.copyOf(combined);
    }

    public String normalizeValue(int tag, String value) {
        if (value == null) {
            return null;
        }
        TagNormalizer normalizer = normalizersByTag.get(tag);
        return normalizer == null ? value : normalizer.apply(value);
    }

    private static Map<String, Set<Integer>> freezeTagMap(Map<String, Set<Integer>> source) {
        Map<String, Set<Integer>> out = new LinkedHashMap<>();
        for (Map.Entry<String, Set<Integer>> entry : source.entrySet()) {
            out.put(entry.getKey(), normalizeTags(entry.getValue()));
        }
        return Map.copyOf(out);
    }

    private static Set<Integer> normalizeTags(Set<Integer> tags) {
        Set<Integer> out = new LinkedHashSet<>();
        for (Integer tag : tags) {
            if (tag == null || tag <= 0) {
                throw new IllegalArgumentException("Tag must be positive: " + tag);
            }
            out.add(tag);
        }
        return Set.copyOf(out);
    }

    public static final class Builder {
        private Set<Integer> defaultIncludeTags;
        private Set<Integer> defaultExcludeTags = DEFAULT_EXCLUDE_TAGS;
        private final Map<String, Set<Integer>> includeByMsgType = new LinkedHashMap<>();
        private final Map<String, Set<Integer>> excludeByMsgType = new LinkedHashMap<>();
        private boolean excludeTimeLikeTags;
        private final Map<Integer, TagNormalizer> normalizers = new LinkedHashMap<>();

        public Builder defaultIncludeTags(Set<Integer> tags) {
            this.defaultIncludeTags = normalizeTags(tags);
            return this;
        }

        public Builder defaultExcludeTags(Set<Integer> tags) {
            this.defaultExcludeTags = normalizeTags(tags);
            return this;
        }

        public Builder includeTagsForMsgType(String msgType, Set<Integer> tags) {
            includeByMsgType.put(msgType, normalizeTags(tags));
            return this;
        }

        public Builder excludeTagsForMsgType(String msgType, Set<Integer> tags) {
            excludeByMsgType.put(msgType, normalizeTags(tags));
            return this;
        }

        public Builder excludeTimeLikeTags(boolean exclude) {
            this.excludeTimeLikeTags = exclude;
            return this;
        }

        public Builder normalizer(int tag, TagNormalizer normalizer) {
            if (tag <= 0) {
                throw new IllegalArgumentException("Tag must be positive: " + tag);
            }
            normalizers.put(tag, normalizer);
            return this;
        }

        public CompareConfig build() {
            return new CompareConfig(
                defaultIncludeTags,
                defaultExcludeTags,
                includeByMsgType,
                excludeByMsgType,
                excludeTimeLikeTags,
                normalizers
            );
        }
    }

    public static final class TagNormalizer {
        private final boolean trim;
        private final List<RegexRule> regexRules;

        private TagNormalizer(boolean trim, List<RegexRule> regexRules) {
            this.trim = trim;
            this.regexRules = List.copyOf(regexRules);
        }

        public static Builder builder() {
            return new Builder();
        }

        public String apply(String value) {
            String out = value;
            if (trim) {
                out = out.trim();
            }
            for (RegexRule rule : regexRules) {
                out = rule.pattern.matcher(out).replaceAll(rule.replacement);
            }
            return out;
        }

        public static final class Builder {
            private boolean trim;
            private final List<RegexRule> regexRules = new ArrayList<>();

            public Builder trim(boolean trim) {
                this.trim = trim;
                return this;
            }

            public Builder regexReplace(String pattern, String replacement) {
                regexRules.add(new RegexRule(Pattern.compile(pattern), Objects.requireNonNull(replacement, "replacement")));
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
