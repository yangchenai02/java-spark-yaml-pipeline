package com.github.cy19890513.etl.features;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Trailing-window aggregation on a local Spark session. */
class WindowAggregateTransformTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .appName("window-aggregate-test")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    private static Timestamp ts(String value) {
        return Timestamp.valueOf(value);
    }

    private Dataset<Row> events() {
        StructType schema = new StructType()
                .add("customerID", DataTypes.StringType, false)
                .add("event_time", DataTypes.TimestampType, false)
                .add("event_type", DataTypes.StringType, false)
                .add("amount", DataTypes.DoubleType, false);
        return spark.createDataFrame(List.of(
                RowFactory.create("c1", ts("2025-01-15 00:00:00"), "support_call", 20.0),
                RowFactory.create("c1", ts("2025-02-15 00:00:00"), "support_call", 10.0),
                RowFactory.create("c1", ts("2025-03-05 00:00:00"), "support_call", 99.0),
                RowFactory.create("c1", ts("2025-03-10 00:00:00"), "cancel_call", 50.0),
                RowFactory.create("c3", ts("2025-01-15 00:00:00"), "support_call", 5.0)),
                schema);
    }

    @Test
    void trailingWindowsEndAtEachRow() {
        Dataset<Row> result = new WindowAggregateTransform().transform(
                Map.of("support_calls", events()),
                Map.of("features", "src/test/resources/features/pit-join.yaml"));

        // One output row per input row, in event order.
        List<Row> rows = result
                .orderBy("customerID", "event_time")
                .collectAsList();
        assertEquals(5, rows.size());

        // Jan 15 is 31 days before Feb 15: outside the 30d window, inside 90d.
        assertRow(rows.get(0), "c1", 1L, 1L, 20.0, 20.0);
        assertRow(rows.get(1), "c1", 1L, 2L, 30.0, 10.0);
        assertRow(rows.get(2), "c1", 2L, 3L, 129.0, 99.0);
        // The cancel call itself is filtered out of the count features,
        // but counts as an amount row for the unfiltered sum/last.
        assertRow(rows.get(3), "c1", 2L, 3L, 179.0, 50.0);
        assertRow(rows.get(4), "c3", 1L, 1L, 5.0, 5.0);
    }

    private static void assertRow(Row row, String customer, long calls30d, long calls90d,
            double spent90d, double last30d) {
        assertEquals(customer, row.getString(row.fieldIndex("customerID")));
        assertEquals(calls30d, (Long) row.getAs("calls_30d"));
        assertEquals(calls90d, (Long) row.getAs("calls_90d"));
        assertEquals(spent90d, (Double) row.getAs("spent_90d"));
        assertEquals(last30d, (Double) row.getAs("last_amount_30d"));
    }

    @Test
    void multipleInputsNeedEventsOption() {
        Dataset<Row> events = events();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new WindowAggregateTransform().transform(
                        Map.of("a", events, "b", events),
                        Map.of("features", "src/test/resources/features/pit-join.yaml")));
        assertTrue(e.getMessage().contains("'events'"));
    }
}
