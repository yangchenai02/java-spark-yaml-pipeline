package com.github.cy19890513.etl.features;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.cy19890513.etl.api.StageRegistry;
import com.github.cy19890513.etl.api.definition.PipelineDefinition;
import com.github.cy19890513.etl.connectors.BuiltinStages;
import com.github.cy19890513.etl.core.PipelineLoader;
import com.github.cy19890513.etl.core.PipelineRunner;
import com.github.cy19890513.etl.quality.QualityChecks;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test: the bundled churn example pipeline, run from its YAML
 * through the real runner — CSV sources, point-in-time join, consistency
 * check against the serving manifest, feature store sink.
 */
class ChurnFeaturesPipelineTest {

    private static SparkSession spark;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .appName("churn-features-e2e-test")
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
    void churnExamplePipeline() throws Exception {
        Path example = Path.of(System.getProperty("user.dir"), "../examples/churn-features")
                .normalize();
        Path work = tempDir.resolve("churn-features");
        try (Stream<Path> walk = Files.walk(example)) {
            for (Path source : walk.filter(Files::isRegularFile).toList()) {
                Path target = work.resolve(example.relativize(source));
                Files.createDirectories(target.getParent());
                Files.copy(source, target);
            }
        }
        Path pipelineYaml = work.resolve("pipeline.yaml");
        String yaml = Files.readString(pipelineYaml)
                .replace("examples/churn-features", work.toString());
        Files.writeString(pipelineYaml, yaml);

        StageRegistry registry = new StageRegistry();
        BuiltinStages.registerAll(registry);
        QualityChecks.registerAll(registry);
        FeatureStages.registerAll(registry);
        PipelineDefinition definition = new PipelineLoader().load(pipelineYaml);
        new PipelineRunner(registry, spark).run(definition);

        Path output = work.resolve("output/churn_features");
        assertTrue(Files.exists(output.resolve(FeatureManifest.FILE_NAME)),
                "feature store manifest should exist");
        FeatureManifest written = FeatureManifest.readFrom(
                output.resolve(FeatureManifest.FILE_NAME));
        assertEquals(List.of("customerID", "tenure", "label_time", "churn",
                        "calls_30d", "calls_90d", "spent_90d"),
                written.featureNames());
        String expectedHash = FeatureManifest.hashOf(
                FeatureRegistry.load(work.resolve("features.yaml")));
        assertEquals(expectedHash, written.definitionHash());
        assertEquals("7a5aa5853d91b916ef87dcf0ca64b19639095d3e9965d5d26bab8463786bf72a",
                expectedHash);

        Dataset<Row> features = spark.read().parquet(output.toString());
        Map<String, Row> rows = new LinkedHashMap<>();
        for (Row row : features.orderBy("customerID").collectAsList()) {
            rows.put(row.getString(0), row);
        }
        Row c1 = rows.get("c1");
        // c1's March 5 call is after its March 1 label time: it must not count.
        assertEquals(1L, (Long) c1.getAs("calls_30d"));
        assertEquals(2L, (Long) c1.getAs("calls_90d"));
        assertEquals(30.0, (Double) c1.getAs("spent_90d"));
        Row c2 = rows.get("c2");
        assertEquals(0L, (Long) c2.getAs("calls_30d"));
        assertTrue(c2.isNullAt(c2.fieldIndex("spent_90d")));
        Row c3 = rows.get("c3");
        assertEquals(1L, (Long) c3.getAs("calls_30d"));
        assertEquals(5.0, (Double) c3.getAs("spent_90d"));
    }
}
