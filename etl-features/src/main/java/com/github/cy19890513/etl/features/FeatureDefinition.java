package com.github.cy19890513.etl.features;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * One feature: a named aggregation over an event stream, computed only
 * from events at or before an entity row's as-of time.
 *
 * <p>Loaded from {@code features.yaml} (see {@link FeatureRegistry}).
 * Example:
 * <pre>
 * - name: calls_30d
 *   source: support_calls
 *   event_time: event_time
 *   window: 30d
 *   aggregation: count
 *   filter: { event_type: support_call }
 * </pre>
 *
 * <p>{@code source} names the pipeline input holding the events (a stage
 * name). {@code filter} is a set of equality predicates on event columns
 * ({@code column == value}), all of which must hold. {@code value_column}
 * is the event column being aggregated; it is required for every
 * aggregation except {@code count}, which counts events and defaults to
 * the {@code event_time} column.
 */
public final class FeatureDefinition {

    /** Aggregations a feature may use. */
    public enum Aggregation {
        COUNT, SUM, AVG, MIN, MAX, LAST
    }

    /** Trailing lookback window, e.g. {@code 30d} or {@code 24h}. */
    public record Window(long amount, Unit unit) {

        /** Window unit. */
        public enum Unit {
            DAYS, HOURS
        }

        private static final Pattern PATTERN = Pattern.compile("^(\\d+)(d|h)$");

        /**
         * Parses a window string.
         *
         * @param text e.g. {@code "30d"}, {@code "24h"}
         * @return the parsed window
         * @throws InvalidFeatureDefinitionException when the format is wrong
         */
        public static Window parse(String text) {
            if (text == null || text.isBlank()) {
                throw new InvalidFeatureDefinitionException(
                        "Feature window must be a non-blank string like '30d' or '24h'");
            }
            Matcher matcher = PATTERN.matcher(text.trim());
            if (!matcher.matches()) {
                throw new InvalidFeatureDefinitionException(
                        "Invalid window '" + text + "': expected '<number>d' or '<number>h', e.g. '30d'");
            }
            long amount = Long.parseLong(matcher.group(1));
            if (amount <= 0) {
                throw new InvalidFeatureDefinitionException(
                        "Invalid window '" + text + "': amount must be positive");
            }
            return new Window(amount,
                    "d".equals(matcher.group(2)) ? Unit.DAYS : Unit.HOURS);
        }

        /** Renders the window as a Spark SQL interval literal, e.g. {@code 30 DAYS}. */
        public String toIntervalSql() {
            return amount + (unit == Unit.DAYS ? " DAYS" : " HOURS");
        }

        @Override
        public String toString() {
            return amount + (unit == Unit.DAYS ? "d" : "h");
        }
    }

    private final String name;
    private final String source;
    private final String eventTime;
    private final Window window;
    private final Aggregation aggregation;
    private final String valueColumn;
    private final Map<String, String> filter;

    /**
     * Creates a feature definition (Jackson calls this; prefer the builder
     * in tests and code).
     */
    @JsonCreator
    public FeatureDefinition(
            @JsonProperty("name") String name,
            @JsonProperty("source") String source,
            @JsonProperty("event_time") String eventTime,
            @JsonProperty("window") String window,
            @JsonProperty("aggregation") Aggregation aggregation,
            @JsonProperty("value_column") String valueColumn,
            @JsonProperty("filter") Map<String, String> filter) {
        if (name == null || name.isBlank()) {
            throw new InvalidFeatureDefinitionException("Feature name must be a non-blank string");
        }
        if (!name.trim().matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new InvalidFeatureDefinitionException(
                    "Feature name '" + name + "' must be a valid SQL identifier "
                            + "(letters, digits, underscore, not starting with a digit): "
                            + "it becomes an output column name");
        }
        if (source == null || source.isBlank()) {
            throw new InvalidFeatureDefinitionException(
                    "Feature '" + name + "' requires a non-blank 'source' (the events input)");
        }
        if (eventTime == null || eventTime.isBlank()) {
            throw new InvalidFeatureDefinitionException(
                    "Feature '" + name + "' requires a non-blank 'event_time' column");
        }
        if (aggregation == null) {
            throw new InvalidFeatureDefinitionException(
                    "Feature '" + name + "' requires an 'aggregation' "
                            + "(count, sum, avg, min, max, last)");
        }
        this.name = name.trim();
        this.source = source.trim();
        this.eventTime = eventTime.trim();
        this.window = Window.parse(window);
        this.aggregation = aggregation;
        if (aggregation == Aggregation.COUNT) {
            this.valueColumn = valueColumn == null || valueColumn.isBlank()
                    ? this.eventTime : valueColumn.trim();
        } else {
            if (valueColumn == null || valueColumn.isBlank()) {
                throw new InvalidFeatureDefinitionException(
                        "Feature '" + name + "' with aggregation '" + aggregation
                                + "' requires a 'value_column'");
            }
            this.valueColumn = valueColumn.trim();
        }
        Map<String, String> copy = new LinkedHashMap<>();
        if (filter != null) {
            for (Map.Entry<String, String> entry : filter.entrySet()) {
                if (entry.getKey() == null || entry.getKey().isBlank()) {
                    throw new InvalidFeatureDefinitionException(
                            "Feature '" + name + "' has a blank filter key");
                }
                copy.put(entry.getKey().trim(),
                        entry.getValue() == null ? null : entry.getValue().trim());
            }
        }
        this.filter = Collections.unmodifiableMap(copy);
    }

    /** Returns the feature name, used as the output column name. */
    public String name() {
        return name;
    }

    /** Returns the events input (pipeline stage name) this feature reads. */
    public String source() {
        return source;
    }

    /** Returns the timestamp column on each event row. */
    public String eventTime() {
        return eventTime;
    }

    /** Returns the trailing lookback window. */
    public Window window() {
        return window;
    }

    /** Returns the aggregation. */
    public Aggregation aggregation() {
        return aggregation;
    }

    /** Returns the event column being aggregated. */
    public String valueColumn() {
        return valueColumn;
    }

    /** Returns the equality filter predicates (column to value), never null. */
    public Map<String, String> filter() {
        return filter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FeatureDefinition that)) return false;
        return Objects.equals(name, that.name)
                && Objects.equals(source, that.source)
                && Objects.equals(eventTime, that.eventTime)
                && Objects.equals(window, that.window)
                && aggregation == that.aggregation
                && Objects.equals(valueColumn, that.valueColumn)
                && Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, source, eventTime, window, aggregation, valueColumn, filter);
    }

    @Override
    public String toString() {
        return "FeatureDefinition{name='" + name + "', source='" + source
                + "', aggregation=" + aggregation + ", window=" + window + "}";
    }
}
