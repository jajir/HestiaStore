package org.hestiastore.index.segmentindex.configuration.tuning;

/**
 * Typed runtime write-path tuning snapshot.
 */
public final class RuntimeWritePathTuningSnapshot {

    private final int segmentWriteCacheKeyLimit;
    private final int segmentWriteCacheKeyLimitDuringMaintenance;
    private final int segmentSplitKeyThreshold;

    RuntimeWritePathTuningSnapshot(final int segmentWriteCacheKeyLimit,
            final int segmentWriteCacheKeyLimitDuringMaintenance,
            final int segmentSplitKeyThreshold) {
        this.segmentWriteCacheKeyLimit = segmentWriteCacheKeyLimit;
        this.segmentWriteCacheKeyLimitDuringMaintenance =
                segmentWriteCacheKeyLimitDuringMaintenance;
        this.segmentSplitKeyThreshold = segmentSplitKeyThreshold;
    }

    public int segmentWriteCacheKeyLimit() {
        return segmentWriteCacheKeyLimit;
    }

    public int segmentWriteCacheKeyLimitDuringMaintenance() {
        return segmentWriteCacheKeyLimitDuringMaintenance;
    }

    public int segmentSplitKeyThreshold() {
        return segmentSplitKeyThreshold;
    }
}
