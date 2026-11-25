package com.github.cy19890513.etl.features;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;

/**
 * The manifest a {@link FeatureStoreSink} writes next to the Parquet
 * files: the feature names in order, their Spark types, a hash of the
 * feature definitions, and the write time. It is the contract the
 * serving side reads — the {@code feature_consistency} check compares
 * a serving feature file against exactly this.
 *
 * <p>JSON shape:
 * <pre>
 * {
 *   "features": [{"name": "calls_30d", "type": "bigint"}, ...],
 *   "definition_hash": "9f2c...",
 *   "created_at": "2026-11-25T19:30:00Z"
 * }
 * </pre>
 */
public final class FeatureManifest {

    /** One feature in the manifest: name and Spark type. */
    public record FeatureEntry(String name, String type) {
    }

    /** File name of the manifest inside a feature store directory. */
    public static final String FILE_NAME = "_features.json";

    private final List<FeatureEntry> features;
    private final String definitionHash;
    private final String createdAt;

    @JsonCreator
    public FeatureManifest(
            @JsonProperty("features") List<FeatureEntry> features,
            @JsonProperty("definition_hash") String definitionHash,
            @JsonProperty("created_at") String createdAt) {
        if (features == null || features.isEmpty()) {
            throw new IllegalArgumentException("A feature manifest requires a non-empty feature list");
        }
        this.features = List.copyOf(features);
        this.definitionHash = definitionHash;
        this.createdAt = createdAt == null ? Instant.now().toString() : createdAt;
    }

    /** Builds a manifest from a DataFrame's schema. */
    public static FeatureManifest fromDataFrame(Dataset<Row> frame, String definitionHash) {
        List<FeatureEntry> entries = new ArrayList<>();
        for (StructField field : frame.schema().fields()) {
            entries.add(new FeatureEntry(field.name(), field.dataType().simpleString()));
        }
        return new FeatureManifest(entries, definitionHash, Instant.now().toString());
    }

    /**
     * Hashes the feature definitions canonically (name, source, event
     * time, window, aggregation, value column, sorted filters, in
     * definition order). Two runs with the same definitions produce the
     * same hash; any definition change changes it.
     */
    public static String hashOf(FeatureRegistry registry) {
        StringBuilder canonical = new StringBuilder();
        for (FeatureDefinition feature : registry.features()) {
            canonical.append(feature.name()).append('|')
                    .append(feature.source()).append('|')
                    .append(feature.eventTime()).append('|')
                    .append(feature.window()).append('|')
                    .append(feature.aggregation()).append('|')
                    .append(feature.valueColumn()).append('|');
            feature.filter().entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> canonical.append(e.getKey()).append('=').append(e.getValue()).append(';'));
            canonical.append('\n');
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(canonical.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required but unavailable", e);
        }
    }

    /** Writes this manifest as {@code _features.json} into the given directory. */
    public void writeTo(Path directory) throws IOException {
        Files.createDirectories(directory);
        new ObjectMapper().writerWithDefaultPrettyPrinter()
                .writeValue(directory.resolve(FILE_NAME).toFile(), this);
    }

    /** Reads a manifest written by {@link #writeTo(Path)}. */
    public static FeatureManifest readFrom(Path file) throws IOException {
        return new ObjectMapper().readValue(file.toFile(), FeatureManifest.class);
    }

    /** Returns the features in order. */
    @JsonProperty("features")
    public List<FeatureEntry> features() {
        return features;
    }

    /** Returns the feature names in order. */
    @JsonIgnore
    public List<String> featureNames() {
        return features.stream().map(FeatureEntry::name).toList();
    }

    /** Returns the definition hash, or null when the manifest was written without one. */
    @JsonProperty("definition_hash")
    public String definitionHash() {
        return definitionHash;
    }

    /** Returns when the manifest was written (ISO-8601). */
    @JsonProperty("created_at")
    public String createdAt() {
        return createdAt;
    }
}
