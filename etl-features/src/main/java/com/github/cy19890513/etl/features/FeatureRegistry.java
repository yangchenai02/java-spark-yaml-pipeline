package com.github.cy19890513.etl.features;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads and validates a {@code features.yaml} file: the single source of
 * truth for feature definitions, shared by the offline (training) run and
 * the online (serving) run so the two can never drift apart.
 *
 * <p>File shape:
 * <pre>
 * entity_key: customerID
 * as_of: label_time
 * features:
 *   - name: calls_30d
 *     source: support_calls
 *     event_time: event_time
 *     window: 30d
 *     aggregation: count
 *     filter: { event_type: support_call }
 * </pre>
 *
 * <p>Validation rejects: a missing or blank {@code entity_key} /
 * {@code as_of}, an empty feature list, duplicate feature names, and any
 * invalid {@link FeatureDefinition} (unknown aggregation, malformed
 * window, missing value column). Unknown top-level keys are ignored, so
 * the file can carry comments and documentation fields.
 */
public final class FeatureRegistry {

    private final String entityKey;
    private final String asOf;
    private final List<FeatureDefinition> features;
    private final Map<String, FeatureDefinition> byName;

    private FeatureRegistry(String entityKey, String asOf, List<FeatureDefinition> features) {
        this.entityKey = entityKey;
        this.asOf = asOf;
        this.features = List.copyOf(features);
        Map<String, FeatureDefinition> map = new LinkedHashMap<>();
        for (FeatureDefinition feature : features) {
            map.put(feature.name(), feature);
        }
        this.byName = Map.copyOf(map);
    }

    /**
     * Loads a registry from a {@code features.yaml} file.
     *
     * @param yamlFile path to the definitions file
     * @return the validated registry
     * @throws IOException when the file cannot be read
     * @throws InvalidFeatureDefinitionException when the content is invalid
     */
    public static FeatureRegistry load(Path yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        mapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        Raw raw;
        try (InputStream in = Files.newInputStream(yamlFile)) {
            raw = mapper.readValue(in, Raw.class);
        } catch (JsonProcessingException e) {
            // Jackson wraps constructor validation failures (e.g. an unknown
            // aggregation or a malformed window) in a mapping exception.
            throw new InvalidFeatureDefinitionException(
                    "Cannot parse feature definitions from " + yamlFile + ": " + e.getMessage(), e);
        }
        if (raw == null) {
            throw new InvalidFeatureDefinitionException(
                    "Empty feature definitions file: " + yamlFile);
        }
        return validate(raw, yamlFile);
    }

    private static FeatureRegistry validate(Raw raw, Path yamlFile) {
        String where = yamlFile.toString();
        if (raw.entityKey == null || raw.entityKey.isBlank()) {
            throw new InvalidFeatureDefinitionException(
                    "Feature definitions in " + where + " require a non-blank 'entity_key'");
        }
        if (raw.asOf == null || raw.asOf.isBlank()) {
            throw new InvalidFeatureDefinitionException(
                    "Feature definitions in " + where + " require a non-blank 'as_of'");
        }
        if (raw.features == null || raw.features.isEmpty()) {
            throw new InvalidFeatureDefinitionException(
                    "Feature definitions in " + where + " require a non-empty 'features' list");
        }
        List<FeatureDefinition> seen = new ArrayList<>();
        for (FeatureDefinition feature : raw.features) {
            if (feature == null) {
                throw new InvalidFeatureDefinitionException(
                        "Feature definitions in " + where + " contain a null feature entry");
            }
            if (seen.stream().anyMatch(f -> f.name().equals(feature.name()))) {
                throw new InvalidFeatureDefinitionException(
                        "Duplicate feature name '" + feature.name() + "' in " + where);
            }
            seen.add(feature);
        }
        return new FeatureRegistry(raw.entityKey.trim(), raw.asOf.trim(), seen);
    }

    /** Returns the entity key column, e.g. {@code customerID}. */
    public String entityKey() {
        return entityKey;
    }

    /** Returns the as-of column: no feature may see events past this time. */
    public String asOf() {
        return asOf;
    }

    /** Returns the features in definition order. */
    public List<FeatureDefinition> features() {
        return features;
    }

    /** Returns the feature names in definition order. */
    public List<String> featureNames() {
        return features.stream().map(FeatureDefinition::name).toList();
    }

    /** Returns the feature with the given name, or null when absent. */
    public FeatureDefinition feature(String name) {
        return byName.get(name);
    }

    /** Raw Jackson binding target; only the validated registry is public. */
    static final class Raw {
        public String entityKey;
        public String asOf;
        public List<FeatureDefinition> features;
    }
}
