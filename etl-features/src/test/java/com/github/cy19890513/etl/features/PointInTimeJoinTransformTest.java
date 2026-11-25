package com.github.cy19890513.etl.features;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
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
import org.junit.jupiter.api.io.TempDir;

/** Point-in-time join on a local Spark session. The leakage test is the point. */
class PointInTimeJoinTransformTest {

    private static SparkSession spark;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .appName("pit-join-test")
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

    private Dataset<Row> entities() {
        StructType schema = new StructType()
                .add("customerID", DataTypes.StringType, false)
                .add("label_time", DataTypes.TimestampType, false);
        return spark.createDataFrame(List.of(
                RowFactory.create("c1", ts("2025-03-01 00:00:00")),
                RowFactory.create("c2", ts("2025-03-01 00:00:00")),
                RowFactory.create("c3", ts("2025-02-01 00:00:00"))), schema);
    }

    private Dataset<Row> events() {
        StructType schema = new StructType()
                .add("customerID", DataTypes.StringType, false)
                .add("event_time", DataTypes.TimestampType, false)
                .add("event_type", DataTypes.StringType, false)
                .add("amount", DataTypes.DoubleType, false);
        return spark.createDataFrame(List.of(
                // c1: one call in 30d, a second in 90d, one AFTER as_of (leakage trap)
                RowFactory.create("c1", ts("2025-02-15 00:00:00"), "support_call", 10.0),
                RowFactory.create("c1", ts("2025-01-15 00:00:00"), "support_call", 20.0),
                RowFactory.create("c1", ts("2025-03-05 00:00:00"), "support_call", 99.0),
                RowFactory.create("c1", ts("2025-03-10 00:00:00"), "cancel_call", 50.0),
                // c3: one call in 30d of its earlier as_of
                RowFactory.create("c3", ts("2025-01-15 00:00:00"), "support_call", 5.0)),
                schema);
    }

    private Map<String, Dataset<Row>> inputs() {
        Map<String, Dataset<Row>> inputs = new LinkedHashMap<>();
        inputs.put("customers", entities());
        inputs.put("support_calls", events());
        return inputs;
    }

    private Map<String, Row> byCustomer(Dataset<Row> result) {
        Map<String, Row> rows = new LinkedHashMap<>();
        for (Row row : result.collectAsList()) {
            rows.put(row.getString(0), row);
        }
        return rows;
    }

    @Test
    void excludesFutureEvents() {
        Dataset<Row> result = new PointInTimeJoinTransform().transform(
                inputs(),
                Map.of("features", "src/test/resources/features/pit-join.yaml",
                        "entities", "customers"));

        Map<String, Row> rows = byCustomer(result);
        assertEquals(3, rows.size());
        Row c1 = rows.get("c1");
        // Feb 15 is the only support call inside 30d of Mar 1; Mar 5 is the future.
        assertEquals(1L, (Long) c1.getAs("calls_30d"));
        // Jan 15 joins it inside 90d.
        assertEquals(2L, (Long) c1.getAs("calls_90d"));
        // Sum is 10 + 20; the future 99 and the cancel call never count.
        assertEquals(30.0, (Double) c1.getAs("spent_90d"));
        // Last in-window amount is the Feb 15 call, not the future one.
        assertEquals(10.0, (Double) c1.getAs("last_amount_30d"));

        Row c3 = rows.get("c3");
        assertEquals(1L, (Long) c3.getAs("calls_30d"));
        assertEquals(1L, (Long) c3.getAs("calls_90d"));
        assertEquals(5.0, (Double) c3.getAs("spent_90d"));
        assertEquals(5.0, (Double) c3.getAs("last_amount_30d"));
    }

    @Test
    void entityWithoutEventsKeepsRow() {
        Dataset<Row> result = new PointInTimeJoinTransform().transform(
                inputs(),
                Map.of("features", "src/test/resources/features/pit-join.yaml",
                        "entities", "customers"));

        Row c2 = byCustomer(result).get("c2");
        assertEquals(0L, (Long) c2.getAs("calls_30d"));
        assertEquals(0L, (Long) c2.getAs("calls_90d"));
        assertTrue(c2.isNullAt(c2.fieldIndex("spent_90d")));
        assertTrue(c2.isNullAt(c2.fieldIndex("last_amount_30d")));
    }

    @Test
    void unknownSourceIsRejected() throws Exception {
        Path file = tempDir.resolve("bad-source.yaml");
        Files.writeString(file, """
                entity_key: customerID
                as_of: label_time
                features:
                  - name: calls_30d
                    source: no_such_table
                    event_time: event_time
                    window: 30d
                    aggregation: count
                """);

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new PointInTimeJoinTransform().transform(
                        inputs(), Map.of("features", file.toString(), "entities", "customers")));
        assertTrue(e.getMessage().contains("no_such_table"));
    }

    @Test
    void missingFeaturesOptionIsRejected() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new PointInTimeJoinTransform().transform(
                        inputs(), Map.of("entities", "customers")));
        assertTrue(e.getMessage().contains("'features'"));
    }
}
