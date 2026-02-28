package io.fixreplay.simulator.artio;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

final class ArtioMutationEngine {
    private static final ArtioMutationEngine DISABLED = new ArtioMutationEngine(false, List.of());
    private static final ObjectMapper RULE_MAPPER = new ObjectMapper(new YAMLFactory());

    private final boolean strictMode;
    private final List<Rule> rules;

    private ArtioMutationEngine(boolean strictMode, List<Rule> rules) {
        this.strictMode = strictMode;
        this.rules = List.copyOf(rules);
    }

    static ArtioMutationEngine fromConfig(ArtioSimulatorConfig.Mutation mutation) {
        Objects.requireNonNull(mutation, "mutation");
        if (!mutation.enabled()) {
            return DISABLED;
        }

        JsonNode root = loadRulesRoot(mutation);
        JsonNode rulesNode = root == null || root.isNull() ? NullNode.instance : root.path("rules");
        if (rulesNode.isMissingNode() || rulesNode.isNull()) {
            if (root != null && root.isArray()) {
                rulesNode = root;
            } else {
                rulesNode = NullNode.instance;
            }
        }

        List<Rule> parsedRules = parseRules(rulesNode);
        return new ArtioMutationEngine(mutation.strictMode(), parsedRules);
    }

    void apply(String msgType, Map<Integer, String> fields) {
        if (rules.isEmpty()) {
            return;
        }
        for (Rule rule : rules) {
            if (rule.matches(msgType, fields, strictMode)) {
                rule.apply(fields, strictMode);
            }
        }
    }

    private static JsonNode loadRulesRoot(ArtioSimulatorConfig.Mutation mutation) {
        return switch (mutation.ruleSource()) {
            case INLINE -> mutation.rulesInline();
            case FILE -> loadRulesFile(mutation.rulesFile());
            case NONE -> NullNode.instance;
        };
    }

    private static JsonNode loadRulesFile(java.nio.file.Path rulesFile) {
        if (rulesFile == null) {
            return NullNode.instance;
        }
        if (!Files.exists(rulesFile)) {
            throw new IllegalArgumentException("Mutation rules file does not exist: " + rulesFile);
        }
        try {
            JsonNode root = RULE_MAPPER.readTree(rulesFile.toFile());
            return root == null ? NullNode.instance : root;
        } catch (IOException failure) {
            throw new IllegalArgumentException("Unable to read mutation rules file: " + rulesFile, failure);
        }
    }

    private static List<Rule> parseRules(JsonNode rulesNode) {
        if (rulesNode == null || rulesNode.isNull() || rulesNode.isMissingNode()) {
            return List.of();
        }
        if (!rulesNode.isArray()) {
            throw new IllegalArgumentException("Mutation rules must be an array");
        }

        List<Rule> parsedRules = new ArrayList<>(rulesNode.size());
        int index = 0;
        for (JsonNode ruleNode : rulesNode) {
            index++;
            parsedRules.add(parseRule(ruleNode, index));
        }
        return parsedRules;
    }

    private static Rule parseRule(JsonNode ruleNode, int index) {
        JsonNode whenNode = node(ruleNode, "when");
        Set<String> msgTypes = normalizeSet(stringList(whenNode, "msgTypes", "msg_types"));
        List<Condition> conditions = parseConditions(node(whenNode, "conditions"), index);
        List<ActionInstruction> actions = parseActions(node(ruleNode, "actions"), index);
        String name = textOrDefault(ruleNode, "rule-" + index, "name");
        return new Rule(name, msgTypes, conditions, actions);
    }

    private static List<Condition> parseConditions(JsonNode conditionsNode, int ruleIndex) {
        if (conditionsNode.isMissingNode() || conditionsNode.isNull()) {
            return List.of();
        }
        if (!conditionsNode.isArray()) {
            throw new IllegalArgumentException("Mutation rule " + ruleIndex + " conditions must be an array");
        }

        List<Condition> conditions = new ArrayList<>(conditionsNode.size());
        for (JsonNode conditionNode : conditionsNode) {
            int tag = intRequired(conditionNode, "tag");
            Boolean exists = booleanOptional(conditionNode, "exists");
            String equals = text(conditionNode, "equals");
            String regex = text(conditionNode, "regex");
            Set<String> tagIn = normalizeSet(stringList(conditionNode, "tagIn", "tag_in"));
            Pattern pattern = regex == null ? null : Pattern.compile(regex);
            conditions.add(new Condition(tag, exists, equals, pattern, tagIn));
        }
        return conditions;
    }

    private static List<ActionInstruction> parseActions(JsonNode actionsNode, int ruleIndex) {
        if (actionsNode.isMissingNode() || actionsNode.isNull()) {
            return List.of();
        }
        if (!actionsNode.isArray()) {
            throw new IllegalArgumentException("Mutation rule " + ruleIndex + " actions must be an array");
        }

        List<ActionInstruction> actions = new ArrayList<>(actionsNode.size());
        for (JsonNode actionNode : actionsNode) {
            ActionType type = ActionType.from(textRequired(actionNode, "type"));
            int tag = intOrDefault(actionNode, -1, "tag");
            int fromTag = intOrDefault(actionNode, -1, "fromTag", "from_tag");
            int toTag = intOrDefault(actionNode, -1, "toTag", "to_tag");
            String value = text(actionNode, "value");
            String patternText = text(actionNode, "pattern");
            String replacement = textOrDefault(actionNode, "", "replacement");
            Pattern pattern = patternText == null ? null : Pattern.compile(patternText);
            actions.add(new ActionInstruction(type, tag, value, pattern, replacement, fromTag, toTag));
        }
        return actions;
    }

    private static JsonNode node(JsonNode parent, String... names) {
        if (parent == null || parent.isNull() || parent.isMissingNode()) {
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
        if (value.isTextual() || value.isNumber() || value.isBoolean()) {
            String normalized = value.asText().trim();
            return normalized.isEmpty() ? null : normalized;
        }
        return null;
    }

    private static String textRequired(JsonNode parent, String... names) {
        String value = text(parent, names);
        if (value == null) {
            throw new IllegalArgumentException("Missing required mutation field: " + List.of(names));
        }
        return value;
    }

    private static String textOrDefault(JsonNode parent, String defaultValue, String... names) {
        String value = text(parent, names);
        return value == null ? defaultValue : value;
    }

    private static int intRequired(JsonNode parent, String... names) {
        int value = intOrDefault(parent, Integer.MIN_VALUE, names);
        if (value <= 0) {
            throw new IllegalArgumentException("Mutation tag must be a positive integer: " + List.of(names));
        }
        return value;
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
        throw new IllegalArgumentException("Mutation integer field has invalid type: " + List.of(names));
    }

    private static Boolean booleanOptional(JsonNode parent, String... names) {
        JsonNode value = node(parent, names);
        if (value.isMissingNode() || value.isNull()) {
            return null;
        }
        if (value.isBoolean()) {
            return value.booleanValue();
        }
        if (value.isTextual()) {
            String text = value.asText().trim();
            if (text.isEmpty()) {
                return null;
            }
            return Boolean.parseBoolean(text);
        }
        return null;
    }

    private static List<String> stringList(JsonNode parent, String... names) {
        JsonNode value = node(parent, names);
        if (value.isMissingNode() || value.isNull()) {
            return List.of();
        }
        if (value.isArray()) {
            List<String> values = new ArrayList<>(value.size());
            for (JsonNode item : value) {
                if (item == null || item.isNull()) {
                    continue;
                }
                String text = item.asText("").trim();
                if (!text.isEmpty()) {
                    values.add(text);
                }
            }
            return values;
        }
        if (value.isTextual()) {
            String raw = value.asText().trim();
            if (raw.isEmpty()) {
                return List.of();
            }
            String[] split = raw.split(",");
            List<String> values = new ArrayList<>(split.length);
            for (String item : split) {
                String trimmed = item.trim();
                if (!trimmed.isEmpty()) {
                    values.add(trimmed);
                }
            }
            return values;
        }
        return List.of();
    }

    private static Set<String> normalizeSet(List<String> values) {
        if (values.isEmpty()) {
            return Set.of();
        }
        LinkedHashSet<String> normalized = new LinkedHashSet<>(values.size());
        for (String value : values) {
            if (value == null) {
                continue;
            }
            String trimmed = value.trim();
            if (!trimmed.isEmpty()) {
                normalized.add(trimmed);
            }
        }
        return normalized.isEmpty() ? Set.of() : Collections.unmodifiableSet(normalized);
    }

    private record Rule(String name, Set<String> msgTypes, List<Condition> conditions, List<ActionInstruction> actions) {
        private Rule {
            Objects.requireNonNull(name, "name");
            msgTypes = Set.copyOf(Objects.requireNonNull(msgTypes, "msgTypes"));
            conditions = List.copyOf(Objects.requireNonNull(conditions, "conditions"));
            actions = List.copyOf(Objects.requireNonNull(actions, "actions"));
        }

        private boolean matches(String msgType, Map<Integer, String> fields, boolean strictMode) {
            if (!msgTypes.isEmpty() && !msgTypes.contains(msgType)) {
                return false;
            }
            for (Condition condition : conditions) {
                if (!condition.matches(name, fields, strictMode)) {
                    return false;
                }
            }
            return true;
        }

        private void apply(Map<Integer, String> fields, boolean strictMode) {
            for (ActionInstruction action : actions) {
                action.apply(name, fields, strictMode);
            }
        }
    }

    private record Condition(
        int tag,
        Boolean exists,
        String equals,
        Pattern regex,
        Set<String> tagIn
    ) {
        private Condition {
            if (tag <= 0) {
                throw new IllegalArgumentException("Condition tag must be positive");
            }
            tagIn = tagIn == null ? Set.of() : Set.copyOf(tagIn);
        }

        private boolean matches(String ruleName, Map<Integer, String> fields, boolean strictMode) {
            String value = fields.get(tag);

            if (exists != null) {
                if (exists.booleanValue()) {
                    if (value == null) {
                        if (strictMode) {
                            throw missingTag(ruleName, tag, "exists=true condition");
                        }
                        return false;
                    }
                } else if (value != null) {
                    return false;
                }
            }

            if (equals != null) {
                if (value == null) {
                    if (strictMode) {
                        throw missingTag(ruleName, tag, "equals condition");
                    }
                    return false;
                }
                if (!value.equals(equals)) {
                    return false;
                }
            }

            if (regex != null) {
                if (value == null) {
                    if (strictMode) {
                        throw missingTag(ruleName, tag, "regex condition");
                    }
                    return false;
                }
                if (!regex.matcher(value).matches()) {
                    return false;
                }
            }

            if (!tagIn.isEmpty()) {
                if (value == null) {
                    if (strictMode) {
                        throw missingTag(ruleName, tag, "tagIn condition");
                    }
                    return false;
                }
                if (!tagIn.contains(value)) {
                    return false;
                }
            }

            return true;
        }
    }

    private enum ActionType {
        SET,
        REMOVE,
        PREFIX,
        SUFFIX,
        REGEX_REPLACE,
        COPY;

        private static ActionType from(String rawType) {
            String normalized = rawType.trim().toUpperCase().replace('-', '_');
            return switch (normalized) {
                case "SET" -> SET;
                case "REMOVE" -> REMOVE;
                case "PREFIX" -> PREFIX;
                case "SUFFIX" -> SUFFIX;
                case "REGEX_REPLACE" -> REGEX_REPLACE;
                case "COPY" -> COPY;
                default -> throw new IllegalArgumentException("Unsupported mutation action type: " + rawType);
            };
        }
    }

    private record ActionInstruction(
        ActionType type,
        int tag,
        String value,
        Pattern pattern,
        String replacement,
        int fromTag,
        int toTag
    ) {
        private ActionInstruction {
            Objects.requireNonNull(type, "type");
            replacement = replacement == null ? "" : replacement;
            validate(type, tag, pattern, fromTag, toTag);
        }

        private void apply(String ruleName, Map<Integer, String> fields, boolean strictMode) {
            switch (type) {
                case SET -> fields.put(tag, value == null ? "" : value);
                case REMOVE -> {
                    if (fields.containsKey(tag)) {
                        fields.remove(tag);
                    } else if (strictMode) {
                        throw missingTag(ruleName, tag, "remove action");
                    }
                }
                case PREFIX -> {
                    String source = fields.get(tag);
                    if (source == null) {
                        if (strictMode) {
                            throw missingTag(ruleName, tag, "prefix action");
                        }
                    } else {
                        fields.put(tag, (value == null ? "" : value) + source);
                    }
                }
                case SUFFIX -> {
                    String source = fields.get(tag);
                    if (source == null) {
                        if (strictMode) {
                            throw missingTag(ruleName, tag, "suffix action");
                        }
                    } else {
                        fields.put(tag, source + (value == null ? "" : value));
                    }
                }
                case REGEX_REPLACE -> {
                    String source = fields.get(tag);
                    if (source == null) {
                        if (strictMode) {
                            throw missingTag(ruleName, tag, "regex_replace action");
                        }
                    } else {
                        fields.put(tag, pattern.matcher(source).replaceAll(replacement));
                    }
                }
                case COPY -> {
                    String source = fields.get(fromTag);
                    if (source == null) {
                        if (strictMode) {
                            throw missingTag(ruleName, fromTag, "copy action");
                        }
                    } else {
                        fields.put(toTag, source);
                    }
                }
            }
        }

        private static void validate(ActionType type, int tag, Pattern pattern, int fromTag, int toTag) {
            switch (type) {
                case SET, REMOVE, PREFIX, SUFFIX -> {
                    if (tag <= 0) {
                        throw new IllegalArgumentException(type + " action requires a positive tag");
                    }
                }
                case REGEX_REPLACE -> {
                    if (tag <= 0) {
                        throw new IllegalArgumentException("REGEX_REPLACE action requires a positive tag");
                    }
                    if (pattern == null) {
                        throw new IllegalArgumentException("REGEX_REPLACE action requires pattern");
                    }
                }
                case COPY -> {
                    if (fromTag <= 0 || toTag <= 0) {
                        throw new IllegalArgumentException("COPY action requires positive fromTag/toTag");
                    }
                }
            }
        }
    }

    private static IllegalStateException missingTag(String ruleName, int tag, String context) {
        return new IllegalStateException(
            "Mutation strict_mode violation in rule '" + ruleName + "': tag " + tag + " is missing for " + context
        );
    }
}
