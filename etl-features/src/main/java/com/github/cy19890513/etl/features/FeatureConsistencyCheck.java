package com.github.cy19890513.etl.features;

import com.github.cy19890513.etl.api.QualityCheck;
import com.github.cy19890513.etl.api.QualityResult;
import com.github.cy19890513.etl.api.Severity;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Compares a training feature DataFrame against a serving manifest
 * ({@code _features.json} written by {@link FeatureStoreSink}): same
 * feature names, same order, same Spark types. Any difference fails
 * the check — the Spark-scale version of the train/serve consistency
 * check that catches a feature computed one way in training and
 * another way in serving.
 *
 * <p>Required option: {@code manifest} (path to the serving
 * {@code _features.json}). Optional option: {@code expected_hash}
 * (the definition hash the manifest must carry; when set, a
 * definition change fails the check even if the names still match).
 * The check's own severity is {@link Severity#FAIL}; the pipeline
 * runner may override it with the YAML {@code on_failure} policy.
 */
public class FeatureConsistencyCheck implements QualityCheck {

    @Override
    public String name() {
        return "feature_consistency";
    }

    @Override
    public String type() {
        return "feature_consistency";
    }

    @Override
    public QualityResult check(Dataset<Row> input, Map<String, String> options) {
        String manifestPath = FeatureSql.requireOption(
                "feature_consistency", options, "manifest");
        FeatureManifest manifest;
        try {
            manifest = FeatureManifest.readFrom(Path.of(manifestPath));
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "The 'feature_consistency' check cannot read manifest '"
                            + manifestPath + "': " + e.getMessage(), e);
        }

        List<String> problems = new ArrayList<>();
        List<FeatureManifest.FeatureEntry> expected = manifest.features();
        String[] actualColumns = input.columns();
        if (actualColumns.length != expected.size()) {
            problems.add("feature count differs: training has " + actualColumns.length
                    + " columns, serving manifest has " + expected.size());
        }
        int compared = Math.min(actualColumns.length, expected.size());
        for (int i = 0; i < compared; i++) {
            String actualName = actualColumns[i];
            String actualType = input.schema().fields()[i].dataType().simpleString();
            FeatureManifest.FeatureEntry want = expected.get(i);
            if (!actualName.equals(want.name())) {
                problems.add("position " + i + ": training has '" + actualName
                        + "', serving manifest has '" + want.name() + "'");
            } else if (!actualType.equals(want.type())) {
                problems.add("feature '" + actualName + "': training type is '" + actualType
                        + "', serving type is '" + want.type() + "'");
            }
        }
        String expectedHash = options.get("expected_hash");
        if (expectedHash != null && !expectedHash.isBlank()) {
            if (!expectedHash.equals(manifest.definitionHash())) {
                problems.add("definition hash differs: expected '" + expectedHash
                        + "', serving manifest has '" + manifest.definitionHash() + "'");
            }
        }

        if (!problems.isEmpty()) {
            return QualityResult.fail("feature_consistency",
                    "training features do not match serving manifest '" + manifestPath + "': "
                            + String.join("; ", problems),
                    Severity.FAIL);
        }
        return QualityResult.pass("feature_consistency",
                "training features match serving manifest '" + manifestPath + "' ("
                        + expected.size() + " features)");
    }
}
