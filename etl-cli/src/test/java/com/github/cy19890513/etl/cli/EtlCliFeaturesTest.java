package com.github.cy19890513.etl.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * CLI smoke test: the bundled churn example pipeline runs end to end
 * through {@link EtlCli#run(String[])} (exit code 0, feature store written).
 * This is the test that proves the feature stages are registered in the CLI.
 */
class EtlCliFeaturesTest {

    @TempDir
    static Path tempDir;

    @Test
    void churnExampleRunsThroughCli() throws Exception {
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

        int exitCode = EtlCli.run(new String[] {
                pipelineYaml.toString(),
                "--master", "local[2]",
                "--conf", "spark.ui.enabled=false",
                "--conf", "spark.sql.shuffle.partitions=2",
        });

        assertEquals(0, exitCode);
        Path output = work.resolve("output/churn_features");
        assertTrue(Files.exists(output.resolve("_features.json")),
                "feature store manifest should exist after the CLI run");
    }
}
