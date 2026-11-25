package com.github.cy19890513.etl.features;

/**
 * Thrown when a feature definition file is invalid: unknown aggregation,
 * malformed window, duplicate feature name, missing required field.
 */
public class InvalidFeatureDefinitionException extends IllegalArgumentException {

    public InvalidFeatureDefinitionException(String message) {
        super(message);
    }

    public InvalidFeatureDefinitionException(String message, Throwable cause) {
        super(message, cause);
    }
}
