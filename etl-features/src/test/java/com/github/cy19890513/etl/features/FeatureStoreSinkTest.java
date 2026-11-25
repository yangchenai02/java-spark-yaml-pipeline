package com.github.cy19890513.etl.features;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Feature store sink: Parquet + manifest round-trip on a local Spark session. */
class FeatureStoreSinkTest {

    private static SparkSession spark;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .appName("feature-store-test")
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

    @Test
    void writesParquetAndManifest() throws Exception {
        Dataset<Row> features = spark.range(3).toDF("customerID")
                .withColumn("calls_30d", org.apache.spark.sql.functions.lit(2L))
                .withColumn("spent_90d", org.apache.spark.sql.functions.lit(12.5));
        Path output = tempDir.resolve("features-out");

        new FeatureStoreSink().write(features, Map.of(
                "path", output.toString(),
                "mode", "overwrite",
                "features", "src/test/resources/features/pit-join.yaml"));

        assertTrue(Files.exists(output.resolve("_features.json")));
        FeatureManifest manifest = FeatureManifest.readFrom(output.resolve("_features.json"));
        assertEquals(List.of("customerID", "calls_30d", "spent_90d"), manifest.featureNames());
        assertEquals("bigint", manifest.features().get(1).type());
        assertEquals("double", manifest.features().get(2).type());
        assertNotNull(manifest.definitionHash());
        assertNotNull(manifest.createdAt());

        Dataset<Row> back = spark.read().parquet(output.toString());
        assertEquals(3, back.count());
        assertEquals(2L, (Long) back.filter("customerID = 1").select("calls_30d").first().get(0));
    }

    @Test
    void manifestWithoutFeaturesFileHasNoHash() throws Exception {
        Dataset<Row> features = spark.range(1).toDF("customerID");
        Path output = tempDir.resolve("features-no-hash");

        new FeatureStoreSink().write(features, Map.of(
                "path", output.toString(), "mode", "overwrite"));

        FeatureManifest manifest = FeatureManifest.readFrom(output.resolve("_features.json"));
        assertEquals(List.of("customerID"), manifest.featureNames());
        assertNull(manifest.definitionHash());
    }

    @Test
    void definitionHashIsStableAndSensitive() throws Exception {
        FeatureRegistry registry = FeatureRegistry.load(
                Path.of("src/test/resources/features/pit-join.yaml"));
        String first = FeatureManifest.hashOf(registry);
        String second = FeatureManifest.hashOf(registry);
        assertEquals(first, second);
        assertEquals(64, first.length());

        Path changed = tempDir.resolve("changed.yaml");
        Files.writeString(changed, Files.readString(
                Path.of("src/test/resources/features/pit-join.yaml"))
                .replace("window: 30d", "window: 31d"));
        String changedHash = FeatureManifest.hashOf(FeatureRegistry.load(changed));
        assertTrue(!first.equals(changedHash));
    }
}
