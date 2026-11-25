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
 * Computes point-in-time features: for every entity row, aggregates only
 * the events at or before the row's as-of time, inside a trailing window.
 * Events after the as-of time are never visible to a feature — that is
 * the whole point: a feature that sees the future looks perfect in
 * training and is worthless in production.
 *
 * <p>Required options: {@code features} (path to a {@code features.yaml}
 * file, see {@link FeatureRegistry}), {@code entities} (the stage name
 * of the entity input). Every other input is an event table, matched to
 * features by the feature's {@code source} (a stage name).
 *
 * <p>Example pipeline fragment:
 * <pre>
 * transforms:
 *   - name: churn_features
 *     type: point_in_time_join
 *     inputs: [customers, support_calls]
 *     options:
 *       features: features.yaml
 *       entities: customers
 * </pre>
 *
 * <p>Implementation: one Spark SQL query per event source. Each query
 * LEFT JOINs the events with the two inequalities that enforce the
 * point-in-time rule ({@code event_time <= as_of} and
 * {@code event_time > as_of - window}), then aggregates per feature
 * with a {@code CASE WHEN} that restricts each feature to its own
 * window (features sharing a source may have different windows). The
 * per-source results are LEFT JOINed back onto the entities, so an
 * entity with no events keeps its row with a zero count and null
 * value-aggregates.
 */
public class PointInTimeJoinTransform implements TransformStage {

    @Override
    public String name() {
        return "point_in_time_join";
    }

    @Override
    public String type() {
        return "point_in_time_join";
    }

    @Override
    public Dataset<Row> transform(Map<String, Dataset<Row>> inputs, Map<String, String> options) {
        String featuresPath = require(options, "features");
        String entitiesName = require(options, "entities");
        FeatureRegistry registry = loadRegistry(featuresPath);

        Dataset<Row> entities = inputs.get(entitiesName);
        if (entities == null) {
            throw new IllegalArgumentException(
                    "The 'point_in_time_join' transform requires an input named '" + entitiesName
                            + "' (option 'entities'), got inputs " + inputs.keySet());
        }
        String key = registry.entityKey();
        String asOf = registry.asOf();
        requireColumn("entities", entities, key);
        requireColumn("entities", entities, asOf);
        for (String column : entities.columns()) {
            if (registry.featureNames().contains(column)) {
                throw new IllegalArgumentException(
                        "The 'point_in_time_join' transform cannot write feature '" + column
                                + "': the entities input already has a column with that name");
            }
        }

        Map<String, List<FeatureDefinition>> bySource = groupBySource(registry, inputs);
        SparkSession spark = entities.sparkSession();
        String uid = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String entitiesView = "__pit_entities_" + uid;
        entities.createOrReplaceTempView(entitiesView);

        List<String> featureSelects = new ArrayList<>();
        List<String> joins = new ArrayList<>();
        int sourceIndex = 0;
        for (Map.Entry<String, List<FeatureDefinition>> group : bySource.entrySet()) {
            String sourceName = group.getKey();
            Dataset<Row> events = inputs.get(sourceName);
            String eventsView = "__pit_events_" + uid + "_" + sourceIndex;
            events.createOrReplaceTempView(eventsView);
            String alias = "s" + sourceIndex;
            for (FeatureDefinition feature : group.getValue()) {
                featureSelects.add(alias + ".`" + feature.name() + "`");
            }
            joins.add("LEFT JOIN (" + aggregateSql(
                    entitiesView, eventsView, key, asOf, group.getValue()) + ") " + alias
                    + " ON " + alias + ".`__key` = e.`" + key + "`"
                    + " AND " + alias + ".`__asof` = e.`" + asOf + "`");
            sourceIndex++;
        }

        String sql = "SELECT e.*, " + String.join(", ", featureSelects)
                + " FROM " + entitiesView + " e " + String.join(" ", joins);
        return spark.sql(sql);
    }

    private static FeatureRegistry loadRegistry(String featuresPath) {
        try {
            return FeatureRegistry.load(Path.of(featuresPath));
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "The 'point_in_time_join' transform cannot load features file '"
                            + featuresPath + "': " + e.getMessage(), e);
        }
    }

    private static Map<String, List<FeatureDefinition>> groupBySource(
            FeatureRegistry registry, Map<String, Dataset<Row>> inputs) {
        Map<String, List<FeatureDefinition>> bySource = new LinkedHashMap<>();
        for (FeatureDefinition feature : registry.features()) {
            Dataset<Row> events = inputs.get(feature.source());
            if (events == null) {
                throw new IllegalArgumentException(
                        "Feature '" + feature.name() + "' reads source '" + feature.source()
                                + "', which is not one of the transform inputs "
                                + inputs.keySet());
            }
            requireColumn("events '" + feature.source() + "'", events, registry.entityKey());
            requireColumn("events '" + feature.source() + "'", events, feature.eventTime());
            if (feature.aggregation() != FeatureDefinition.Aggregation.COUNT
                    || !feature.valueColumn().equals(feature.eventTime())) {
                requireColumn("events '" + feature.source() + "'", events, feature.valueColumn());
            }
            for (String column : feature.filter().keySet()) {
                requireColumn("events '" + feature.source() + "'", events, column);
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
     * Builds the per-source aggregate subquery. The JOIN uses the widest
     * window in the group; each feature's own window is enforced again
     * inside its aggregate expression, so features sharing a source can
     * have different windows.
     */
    private static String aggregateSql(String entitiesView, String eventsView,
            String key, String asOf, List<FeatureDefinition> features) {
        long maxHours = features.stream()
                .mapToLong(f -> f.window().unit() == FeatureDefinition.Window.Unit.DAYS
                        ? f.window().amount() * 24 : f.window().amount())
                .max().orElseThrow();
        String maxInterval = maxHours % 24 == 0
                ? (maxHours / 24) + " DAYS" : maxHours + " HOURS";

        List<String> selects = new ArrayList<>();
        selects.add("e.`" + key + "` AS `__key`");
        selects.add("e.`" + asOf + "` AS `__asof`");
        for (FeatureDefinition feature : features) {
            selects.add(aggregateExpr(feature, key, asOf) + " AS `" + feature.name() + "`");
        }
        return "SELECT " + String.join(", ", selects)
                + " FROM " + entitiesView + " e"
                + " LEFT JOIN " + eventsView + " ev"
                + " ON ev.`" + key + "` = e.`" + key + "`"
                + " AND ev.`" + features.get(0).eventTime() + "` <= e.`" + asOf + "`"
                + " AND ev.`" + features.get(0).eventTime() + "` > e.`" + asOf
                + "` - INTERVAL " + maxInterval
                + " GROUP BY e.`" + key + "`, e.`" + asOf + "`";
    }

    private static String aggregateExpr(FeatureDefinition feature, String key, String asOf) {
        String inWindow = windowPredicate(feature, asOf);
        String value = "ev.`" + feature.valueColumn() + "`";
        String guarded = "CASE WHEN " + inWindow + " THEN " + value + " END";
        return switch (feature.aggregation()) {
            case COUNT -> "COUNT(" + guarded + ")";
            case SUM -> "SUM(" + guarded + ")";
            case AVG -> "AVG(" + guarded + ")";
            case MIN -> "MIN(" + guarded + ")";
            case MAX -> "MAX(" + guarded + ")";
            // Most recent in-window value: max_by ignores NULL first
            // arguments, so out-of-window rows contribute nothing.
            case LAST -> "MAX_BY(" + guarded + ", ev.`" + feature.eventTime() + "`)";
        };
    }

    private static String windowPredicate(FeatureDefinition feature, String asOf) {
        String eventTime = "ev.`" + feature.eventTime() + "`";
        StringBuilder predicate = new StringBuilder();
        predicate.append(eventTime).append(" <= e.`").append(asOf).append("`");
        predicate.append(" AND ").append(eventTime).append(" > e.`").append(asOf)
                .append("` - INTERVAL ").append(feature.window().toIntervalSql());
        for (Map.Entry<String, String> filter : feature.filter().entrySet()) {
            predicate.append(" AND ev.`").append(filter.getKey()).append("` = ")
                    .append(literal(filter.getValue()));
        }
        return predicate.toString();
    }

    /** Renders a filter value as a SQL literal: numbers unquoted, else a quoted string. */
    private static String literal(String value) {
        if (value != null && value.matches("-?\\d+(\\.\\d+)?")) {
            return value;
        }
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return value.toLowerCase();
        }
        return "'" + (value == null ? "" : value.replace("'", "''")) + "'";
    }

    private static void requireColumn(String where, Dataset<Row> frame, String column) {
        for (String existing : frame.columns()) {
            if (existing.equals(column)) {
                return;
            }
        }
        throw new IllegalArgumentException(
                "The 'point_in_time_join' transform requires column '" + column
                        + "' in " + where + ", got columns " + List.of(frame.columns()));
    }

    private static String require(Map<String, String> options, String key) {
        String value = options.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "The 'point_in_time_join' transform requires option '" + key + "'");
        }
        return value;
    }
}
