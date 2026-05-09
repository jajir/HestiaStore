package org.hestiastore.index.segmentindex.configuration.tuning;

/**
 * Runtime tuning apply outcome.
 */
public enum RuntimeTuningApplyStatus {
    /** Patch was valid and applied atomically. */
    APPLIED,
    /** Patch was rejected and did not mutate runtime state. */
    REJECTED
}
