package org.hestiastore.index.segmentindex.tuning;

/**
 * Typed runtime write-path tuning snapshot.
 */
public final class RuntimeWritePathTuningSnapshot {

    private final Integer segmentWriteCacheKeyLimit;
    private final Integer maintenanceWriteCacheKeyLimit;
    private final Integer indexBufferedWriteKeyLimit;
    private final Integer segmentSplitKeyThreshold;

    RuntimeWritePathTuningSnapshot(final Integer segmentWriteCacheKeyLimit,
            final Integer maintenanceWriteCacheKeyLimit,
            final Integer indexBufferedWriteKeyLimit,
            final Integer segmentSplitKeyThreshold) {
        this.segmentWriteCacheKeyLimit = segmentWriteCacheKeyLimit;
        this.maintenanceWriteCacheKeyLimit = maintenanceWriteCacheKeyLimit;
        this.indexBufferedWriteKeyLimit = indexBufferedWriteKeyLimit;
        this.segmentSplitKeyThreshold = segmentSplitKeyThreshold;
    }

    public Integer segmentWriteCacheKeyLimit() {
        return segmentWriteCacheKeyLimit;
    }

    public Integer getSegmentWriteCacheKeyLimit() {
        return segmentWriteCacheKeyLimit;
    }

    public Integer maintenanceWriteCacheKeyLimit() {
        return maintenanceWriteCacheKeyLimit;
    }

    public Integer getMaintenanceWriteCacheKeyLimit() {
        return maintenanceWriteCacheKeyLimit;
    }

    public Integer indexBufferedWriteKeyLimit() {
        return indexBufferedWriteKeyLimit;
    }

    public Integer getIndexBufferedWriteKeyLimit() {
        return indexBufferedWriteKeyLimit;
    }

    public Integer segmentSplitKeyThreshold() {
        return segmentSplitKeyThreshold;
    }

    public Integer getSegmentSplitKeyThreshold() {
        return segmentSplitKeyThreshold;
    }
}
