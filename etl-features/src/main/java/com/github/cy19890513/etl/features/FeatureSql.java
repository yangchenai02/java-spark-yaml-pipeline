package com.github.cy19890513.etl.features;

import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Shared Spark SQL building blocks for the feature transforms.
 *
 * <p>The point-in-time join and the window aggregate both compute, per
 * feature, an aggregate over events inside a trailing window ending at a
 * row's reference time ({@code as_of} for the join, the row's own
 * {@code event_time} for the window aggregate). The SQL shape is the
 * same; only the row alias differs.
 */
final class FeatureSql {

    private FeatureSql() {
        // utility class
    }

    /** Returns the window length in hours (days count as 24 hours). */
    static long windowHours(FeatureDefinition.Window window) {
        return window.unit() == FeatureDefinition.Window.Unit.DAYS
                ? window.amount() * 24 : window.amount();
    }

    /**
     * Builds one feature's aggregate expression.
     *
     * @param feature    feature definition
     * @param rowAlias   alias of the row whose reference time bounds the window
     *                   (e.g. {@code e} for entities, {@code t} for events)
     * @param eventAlias alias of the events table (e.g. {@code ev})
     * @param referenceTime column on the row holding the reference time
     *                   ({@code as_of} for the join, {@code event_time} for
     *                   the window aggregate)
     * @return SQL expression, e.g.
     *         {@code COUNT(CASE WHEN ev.event_time <= e.label_time AND ... THEN ev.event_time END)}
     */
    static String aggregateExpr(FeatureDefinition feature, String rowAlias,
            String eventAlias, String referenceTime) {
        String inWindow = windowPredicate(feature, rowAlias, eventAlias, referenceTime);
        String value = eventAlias + ".`" + feature.valueColumn() + "`";
        String guarded = "CASE WHEN " + inWindow + " THEN " + value + " END";
        return switch (feature.aggregation()) {
            case COUNT -> "COUNT(" + guarded + ")";
            case SUM -> "SUM(" + guarded + ")";
            case AVG -> "AVG(" + guarded + ")";
            case MIN -> "MIN(" + guarded + ")";
            case MAX -> "MAX(" + guarded + ")";
            // Most recent in-window value: max_by ignores NULL first
            // arguments, so out-of-window rows contribute nothing.
            case LAST -> "MAX_BY(" + guarded + ", " + eventAlias + ".`" + feature.eventTime() + "`)";
        };
    }

    /**
     * The per-feature window predicate: the event is at or before the
     * row's reference time, strictly after reference time minus the
     * feature's window, and matches every filter equality.
     */
    static String windowPredicate(FeatureDefinition feature, String rowAlias,
            String eventAlias, String referenceTime) {
        String eventTime = eventAlias + ".`" + feature.eventTime() + "`";
        String reference = rowAlias + ".`" + referenceTime + "`";
        StringBuilder predicate = new StringBuilder();
        predicate.append(eventTime).append(" <= ").append(reference);
        predicate.append(" AND ").append(eventTime).append(" > ").append(reference)
                .append(" - INTERVAL ").append(feature.window().toIntervalSql());
        for (Map.Entry<String, String> filter : feature.filter().entrySet()) {
            predicate.append(" AND ").append(eventAlias).append(".`").append(filter.getKey())
                    .append("` = ").append(literal(filter.getValue()));
        }
        return predicate.toString();
    }

    /** Renders a filter value as a SQL literal: numbers unquoted, else a quoted string. */
    static String literal(String value) {
        if (value != null && value.matches("-?\\d+(\\.\\d+)?")) {
            return value;
        }
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return value.toLowerCase();
        }
        return "'" + (value == null ? "" : value.replace("'", "''")) + "'";
    }

    /** Throws unless the DataFrame has the given column. */
    static void requireColumn(String stage, String where, Dataset<Row> frame, String column) {
        for (String existing : frame.columns()) {
            if (existing.equals(column)) {
                return;
            }
        }
        throw new IllegalArgumentException(
                "The '" + stage + "' stage requires column '" + column
                        + "' in " + where + ", got columns " + List.of(frame.columns()));
    }

    /** Returns a required string option or throws. */
    static String requireOption(String stage, Map<String, String> options, String key) {
        String value = options.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "The '" + stage + "' stage requires option '" + key + "'");
        }
        return value;
    }
}
