package com.github.cy19890513.etl.features;

import com.github.cy19890513.etl.api.SinkStage;
import com.github.cy19890513.etl.connectors.ParquetSink;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Writes features as Parquet plus a {@link FeatureManifest}
 * ({@code _features.json}) in the same directory: the feature names in
 * order, their Spark types, a hash of the feature definitions, and the
 * write time. The manifest is what the serving side reads — it is the
 * contract, not an afterthought.
 *
 * <p>Required option: {@code path}. Optional: {@code mode} (default
 * {@code errorIfExists}), {@code partitionBy} (e.g. {@code [dt]}),
 * {@code features} (path to the {@code features.yaml} used to compute
 * the definition hash; without it the manifest carries no hash).
 * Every other option is passed to the Parquet writer as-is.
 */
public class FeatureStoreSink implements SinkStage {

    @Override
    public String name() {
        return "feature_store";
    }

    @Override
    public String type() {
        return "feature_store";
    }

    @Override
    public void write(Dataset<Row> input, Map<String, String> options) {
        String path = FeatureSql.requireOption("feature_store", options, "path");
        String definitionHash = null;
        String featuresPath = options.get("features");
        if (featuresPath != null && !featuresPath.isBlank()) {
            try {
                definitionHash = FeatureManifest.hashOf(FeatureRegistry.load(Path.of(featuresPath)));
            } catch (IOException e) {
                throw new IllegalArgumentException(
                        "The 'feature_store' sink cannot load features file '"
                                + featuresPath + "': " + e.getMessage(), e);
            }
        }

        Map<String, String> parquetOptions = new HashMap<>(options);
        parquetOptions.remove("features");
        new ParquetSink().write(input, parquetOptions);

        try {
            FeatureManifest.fromDataFrame(input, definitionHash).writeTo(Path.of(path));
        } catch (IOException e) {
            throw new IllegalStateException(
                    "The 'feature_store' sink wrote Parquet to '" + path
                            + "' but could not write the manifest: " + e.getMessage(), e);
        }
    }
}
