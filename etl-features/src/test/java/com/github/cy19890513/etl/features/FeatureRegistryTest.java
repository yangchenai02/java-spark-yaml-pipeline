package com.github.cy19890513.etl.features;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Loads and validates {@code features.yaml} files; no Spark needed. */
class FeatureRegistryTest {

    @TempDir
    static Path tempDir;

    private Path resource(String name) {
        return Path.of("src/test/resources/features", name);
    }

    @Test
    void loadsValidFile() throws Exception {
        FeatureRegistry registry = FeatureRegistry.load(resource("valid.yaml"));

        assertEquals("customerID", registry.entityKey());
        assertEquals("label_time", registry.asOf());
        assertEquals(
                java.util.List.of("calls_30d", "calls_90d", "total_spent_90d"),
                registry.featureNames());
        FeatureDefinition calls30d = registry.feature("calls_30d");
        assertEquals(FeatureDefinition.Aggregation.COUNT, calls30d.aggregation());
        assertEquals("30 DAYS", calls30d.window().toIntervalSql());
        assertEquals("event_time", calls30d.valueColumn());
        assertEquals("support_call", calls30d.filter().get("event_type"));
        FeatureDefinition spent = registry.feature("total_spent_90d");
        assertEquals(FeatureDefinition.Aggregation.SUM, spent.aggregation());
        assertEquals("amount", spent.valueColumn());
    }

    @Test
    void duplicateNameIsRejected() throws Exception {
        Path file = tempDir.resolve("dup.yaml");
        Files.writeString(file, """
                entity_key: customerID
                as_of: label_time
                features:
                  - name: calls_30d
                    source: events
                    event_time: event_time
                    window: 30d
                    aggregation: count
                  - name: calls_30d
                    source: events
                    event_time: event_time
                    window: 90d
                    aggregation: count
                """);

        InvalidFeatureDefinitionException e = assertThrows(
                InvalidFeatureDefinitionException.class, () -> FeatureRegistry.load(file));
        assertTrue(e.getMessage().contains("Duplicate feature name 'calls_30d'"));
    }

    @Test
    void unknownAggregationIsRejected() throws Exception {
        Path file = tempDir.resolve("bad-agg.yaml");
        Files.writeString(file, """
                entity_key: customerID
                as_of: label_time
                features:
                  - name: calls_30d
                    source: events
                    event_time: event_time
                    window: 30d
                    aggregation: median
                """);

        assertThrows(InvalidFeatureDefinitionException.class,
                () -> FeatureRegistry.load(file));
    }

    @Test
    void malformedWindowIsRejected() throws Exception {
        Path file = tempDir.resolve("bad-window.yaml");
        Files.writeString(file, """
                entity_key: customerID
                as_of: label_time
                features:
                  - name: calls_30d
                    source: events
                    event_time: event_time
                    window: 30
                    aggregation: count
                """);

        InvalidFeatureDefinitionException e = assertThrows(
                InvalidFeatureDefinitionException.class, () -> FeatureRegistry.load(file));
        assertTrue(e.getMessage().contains("Invalid window '30'"));
    }

    @Test
    void sumWithoutValueColumnIsRejected() throws Exception {
        Path file = tempDir.resolve("no-value.yaml");
        Files.writeString(file, """
                entity_key: customerID
                as_of: label_time
                features:
                  - name: total_spent
                    source: events
                    event_time: event_time
                    window: 90d
                    aggregation: sum
                """);

        InvalidFeatureDefinitionException e = assertThrows(
                InvalidFeatureDefinitionException.class, () -> FeatureRegistry.load(file));
        assertTrue(e.getMessage().contains("requires a 'value_column'"));
    }

    @Test
    void missingEntityKeyIsRejected() throws Exception {
        Path file = tempDir.resolve("no-key.yaml");
        Files.writeString(file, """
                as_of: label_time
                features:
                  - name: calls_30d
                    source: events
                    event_time: event_time
                    window: 30d
                    aggregation: count
                """);

        assertThrows(InvalidFeatureDefinitionException.class,
                () -> FeatureRegistry.load(file));
    }

    @Test
    void windowUnitsParse() {
        assertEquals("30 DAYS", FeatureDefinition.Window.parse("30d").toIntervalSql());
        assertEquals("24 HOURS", FeatureDefinition.Window.parse("24h").toIntervalSql());
        assertThrows(InvalidFeatureDefinitionException.class,
                () -> FeatureDefinition.Window.parse("0d"));
        assertThrows(InvalidFeatureDefinitionException.class,
                () -> FeatureDefinition.Window.parse("30x"));
    }

    @Test
    void invalidFeatureNameIsRejected() {
        assertThrows(InvalidFeatureDefinitionException.class,
                () -> new FeatureDefinition("calls 30d", "events", "event_time",
                        "30d", FeatureDefinition.Aggregation.COUNT, null, null));
        assertThrows(InvalidFeatureDefinitionException.class,
                () -> new FeatureDefinition("30d-calls", "events", "event_time",
                        "30d", FeatureDefinition.Aggregation.COUNT, null, null));
    }
}
