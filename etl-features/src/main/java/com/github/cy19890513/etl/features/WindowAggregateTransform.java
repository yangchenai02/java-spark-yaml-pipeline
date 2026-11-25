package com.github.cy19890513.etl.features;

import com.github.cy19890513.etl.api.TransformStage;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Trailing-window aggregation over an event stream, without an entity
 * table: for every event row, aggregates the events of the same entity
 * inside a window ending at that row's own {@code event_time}. The
 * point-in-time rule still holds — the window never sees the future —
 * but the reference time is the row itself, not a separate as-of
 * column (the registry's {@code as_of} is ignored here).
 *
 * <p>Required option: {@code features} (path to a {@code features.yaml}
 * file, see {@link FeatureRegistry}). Optional option: {@code events}
 * (the stage name of the base event input; defaults to the only input
 * when there is exactly one). Every input is an event table, matched
 * to features by the feature's {@code source} (a stage name).
 *
 * <p>The output keeps every input row of the base table, with the
 * feature columns appended — the same "no row is ever dropped" rule
 * as the point-in-time join.
 */
public class WindowAggregateTransform implements TransformStage {

    @Override
    public String name() {
        return "window_aggregate";
    }

    @Override
    public String type() {
        return "window_aggregate";
    }

    @Override
    public Dataset<Row> transform(Map<String, Dataset<Row>> inputs, Map<String, String> options) {
        String featuresPath = FeatureSql.requireOption("window_aggregate", options, "features");
        FeatureRegistry registry = loadRegistry(featuresPath);
        String key = registry.entityKey();

        String baseName = options.get("events");
        if (baseName == null || baseName.isBlank()) {
            if (inputs.size() != 1) {
                throw new IllegalArgumentException(
                        "The 'window_aggregate' transform needs option 'events' when it has "
                                + "more than one input, got inputs " + inputs.keySet());
            }
            baseName = inputs.keySet().iterator().next();
        }
        Dataset<Row> base = inputs.get(baseName);
        if (base == null) {
            throw new IllegalArgumentException(
                    "The 'window_aggregate' transform requires an input named '" + baseName
                            + "' (option 'events'), got inputs " + inputs.keySet());
        }
        String baseEventTime = firstEventTime(registry, baseName);
        FeatureSql.requireColumn("window_aggregate", "events '" + baseName + "'", base, key);
        FeatureSql.requireColumn("window_aggregate",
                "events '" + baseName + "'", base, baseEventTime);
        for (String column : base.columns()) {
            if (registry.featureNames().contains(column)) {
                throw new IllegalArgumentException(
                        "The 'window_aggregate' transform cannot write feature '" + column
                                + "': the events input already has a column with that name");
            }
        }

        Map<String, List<FeatureDefinition>> bySource = groupBySource(registry, inputs, key);
        SparkSession spark = base.sparkSession();
        String uid = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String baseView = "__win_base_" + uid;
        base.createOrReplaceTempView(baseView);

        List<String> featureSelects = new ArrayList<>();
        List<String> joins = new ArrayList<>();
        int sourceIndex = 0;
        for (Map.Entry<String, List<FeatureDefinition>> group : bySource.entrySet()) {
            Dataset<Row> events = inputs.get(group.getKey());
            String eventsView = "__win_events_" + uid + "_" + sourceIndex;
            events.createOrReplaceTempView(eventsView);
            String alias = "s" + sourceIndex;
            for (FeatureDefinition feature : group.getValue()) {
                featureSelects.add(alias + ".`" + feature.name() + "`");
            }
            String eventTime = group.getValue().get(0).eventTime();
            joins.add("LEFT JOIN (" + aggregateSql(eventsView, key, eventTime, group.getValue())
                    + ") " + alias
                    + " ON " + alias + ".`__key` = b.`" + key + "`"
                    + " AND " + alias + ".`__asof` = b.`" + baseEventTime + "`");
            sourceIndex++;
        }

        String sql = "SELECT b.*, " + String.join(", ", featureSelects)
                + " FROM " + baseView + " b " + String.join(" ", joins);
        return spark.sql(sql);
    }

    private static String firstEventTime(FeatureRegistry registry, String baseName) {
        return registry.features().stream()
                .filter(f -> f.source().equals(baseName))
                .map(FeatureDefinition::eventTime)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "No feature in the registry reads source '" + baseName + "'"));
    }

    private static FeatureRegistry loadRegistry(String featuresPath) {
        try {
            return FeatureRegistry.load(Path.of(featuresPath));
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "The 'window_aggregate' transform cannot load features file '"
                            + featuresPath + "': " + e.getMessage(), e);
        }
    }

    private static Map<String, List<FeatureDefinition>> groupBySource(
            FeatureRegistry registry, Map<String, Dataset<Row>> inputs, String key) {
        Map<String, List<FeatureDefinition>> bySource = new LinkedHashMap<>();
        for (FeatureDefinition feature : registry.features()) {
            Dataset<Row> events = inputs.get(feature.source());
            if (events == null) {
                throw new IllegalArgumentException(
                        "Feature '" + feature.name() + "' reads source '" + feature.source()
                                + "', which is not one of the transform inputs "
                                + inputs.keySet());
            }
            FeatureSql.requireColumn("window_aggregate",
                    "events '" + feature.source() + "'", events, key);
            FeatureSql.requireColumn("window_aggregate",
                    "events '" + feature.source() + "'", events, feature.eventTime());
            if (feature.aggregation() != FeatureDefinition.Aggregation.COUNT
                    || !feature.valueColumn().equals(feature.eventTime())) {
                FeatureSql.requireColumn("window_aggregate",
                        "events '" + feature.source() + "'", events, feature.valueColumn());
            }
            for (String column : feature.filter().keySet()) {
                FeatureSql.requireColumn("window_aggregate",
                        "events '" + feature.source() + "'", events, column);
            }
            bySource.computeIfAbsent(feature.source(), k -> new ArrayList<>()).add(feature);
        }
        for (Map.Entry<String, List<FeatureDefinition>> group : bySource.entrySet()) {
            String eventTime = group.getValue().get(0).eventTime();
            for (FeatureDefinition feature : group.getValue()) {
                if (!feature.eventTime().equals(eventTime)) {
                    throw new IllegalArgumentException(
                            "Features sharing source '" + group.getKey() + "' must use the same "
                                    + "'event_time' column, got '" + eventTime + "' and '"
                                    + feature.eventTime() + "'");
                }
            }
        }
        return bySource;
    }

    /**
     * Builds the per-source aggregate subquery: one row per (key,
     * event_time), with each feature aggregated over the trailing window
     * ending at that row's event time. The self-JOIN uses the widest
     * window in the group; each feature's own window is enforced again
     * inside its aggregate expression.
     */
    private static String aggregateSql(String eventsView, String key, String eventTime,
            List<FeatureDefinition> features) {
        long maxHours = features.stream()
                .mapToLong(f -> FeatureSql.windowHours(f.window()))
                .max().orElseThrow();
        String maxInterval = maxHours % 24 == 0
                ? (maxHours / 24) + " DAYS" : maxHours + " HOURS";

        List<String> selects = new ArrayList<>();
        selects.add("t.`" + key + "` AS `__key`");
        selects.add("t.`" + eventTime + "` AS `__asof`");
        for (FeatureDefinition feature : features) {
            selects.add(FeatureSql.aggregateExpr(feature, "t", "ev", eventTime)
                    + " AS `" + feature.name() + "`");
        }
        return "SELECT " + String.join(", ", selects)
                + " FROM " + eventsView + " t"
                + " LEFT JOIN " + eventsView + " ev"
                + " ON ev.`" + key + "` = t.`" + key + "`"
                + " AND ev.`" + eventTime + "` <= t.`" + eventTime + "`"
                + " AND ev.`" + eventTime + "` > t.`" + eventTime + "` - INTERVAL " + maxInterval
                + " GROUP BY t.`" + key + "`, t.`" + eventTime + "`";
    }
}
