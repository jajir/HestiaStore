package org.hestiastore.index.segmentindex.configuration.tuning;

/**
 * Supported value types for runtime tuning fields.
 */
public enum RuntimeTuningValueType {
    /** Integer tuning value. */
    INT,
    /** Long tuning value. */
    LONG,
    /** Boolean tuning value. */
    BOOLEAN,
    /** Double tuning value. */
    DOUBLE,
    /** String tuning value. */
    STRING,
    /** Enum tuning value represented by its constant name. */
    ENUM
}
