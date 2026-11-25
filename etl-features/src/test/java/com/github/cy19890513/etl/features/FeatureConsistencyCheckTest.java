package com.github.cy19890513.etl.features;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Train/serve consistency check against a serving manifest. */
class FeatureConsistencyCheckTest {

    private static SparkSession spark;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .appName("feature-consistency-test")
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

    /** Writes a serving manifest for the given frame and returns its path. */
    private Path servingManifest(Dataset<Row> frame, String definitionHash) throws Exception {
        Path dir = tempDir.resolve("serving-" + System.nanoTime());
        FeatureManifest.fromDataFrame(frame, definitionHash).writeTo(dir);
        return dir.resolve(FeatureManifest.FILE_NAME);
    }

    @Test
    void passesWhenFeaturesMatch() throws Exception {
        Dataset<Row> training = spark.range(2).toDF("customerID")
                .withColumn("calls_30d", org.apache.spark.sql.functions.lit(1L));
        Path manifest = servingManifest(training, "abc123");

        var result = new FeatureConsistencyCheck().check(training,
                Map.of("manifest", manifest.toString(), "expected_hash", "abc123"));

        assertTrue(result.passed());
        assertTrue(result.message().contains("2 features"));
    }

    @Test
    void failsOnRenamedFeature() throws Exception {
        Dataset<Row> training = spark.range(2).toDF("customerID")
                .withColumn("calls_30d", org.apache.spark.sql.functions.lit(1L));
        Dataset<Row> serving = spark.range(2).toDF("customerID")
                .withColumn("calls_7d", org.apache.spark.sql.functions.lit(1L));
        Path manifest = servingManifest(serving, null);

        var result = new FeatureConsistencyCheck().check(training,
                Map.of("manifest", manifest.toString()));

        assertFalse(result.passed());
        assertTrue(result.message().contains("calls_30d"));
        assertTrue(result.message().contains("calls_7d"));
    }

    @Test
    void failsOnTypeChange() throws Exception {
        Dataset<Row> training = spark.range(2).toDF("customerID")
                .withColumn("calls_30d", org.apache.spark.sql.functions.lit(1L));
        Dataset<Row> serving = spark.range(2).toDF("customerID")
                .withColumn("calls_30d", org.apache.spark.sql.functions.lit(1.5));
        Path manifest = servingManifest(serving, null);

        var result = new FeatureConsistencyCheck().check(training,
                Map.of("manifest", manifest.toString()));

        assertFalse(result.passed());
        assertTrue(result.message().contains("bigint"));
        assertTrue(result.message().contains("double"));
    }

    @Test
    void failsOnDefinitionHashChange() throws Exception {
        Dataset<Row> training = spark.range(2).toDF("customerID")
                .withColumn("calls_30d", org.apache.spark.sql.functions.lit(1L));
        Path manifest = servingManifest(training, "old-hash");

        var result = new FeatureConsistencyCheck().check(training,
                Map.of("manifest", manifest.toString(), "expected_hash", "new-hash"));

        assertFalse(result.passed());
        assertTrue(result.message().contains("definition hash differs"));
    }

    @Test
    void missingManifestIsRejected() {
        Dataset<Row> training = spark.range(1).toDF("customerID");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new FeatureConsistencyCheck().check(training,
                        Map.of("manifest", tempDir.resolve("nope.json").toString())));
        assertTrue(e.getMessage().contains("cannot read manifest"));
    }

    @Test
    void missingManifestOptionIsRejected() {
        Dataset<Row> training = spark.range(1).toDF("customerID");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new FeatureConsistencyCheck().check(training, Map.of()));
        assertTrue(e.getMessage().contains("'manifest'"));
    }
}
