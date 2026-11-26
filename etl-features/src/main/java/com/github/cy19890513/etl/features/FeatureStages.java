package com.github.cy19890513.etl.features;

import com.github.cy19890513.etl.api.StageRegistry;

/**
 * Registers every feature-engineering stage with a {@link StageRegistry}.
 */
public final class FeatureStages {

    private FeatureStages() {
        // utility class
    }

    /**
     * Registers the transforms ({@code point_in_time_join},
     * {@code window_aggregate}), the {@code feature_store} sink, and the
     * {@code feature_consistency} quality check.
     *
     * @param registry the registry to populate
     */
    public static void registerAll(StageRegistry registry) {
        registry.registerTransform("point_in_time_join", PointInTimeJoinTransform::new);
        registry.registerTransform("window_aggregate", WindowAggregateTransform::new);

        registry.registerSink("feature_store", FeatureStoreSink::new);

        registry.registerQualityCheck("feature_consistency", FeatureConsistencyCheck::new);
    }
}
