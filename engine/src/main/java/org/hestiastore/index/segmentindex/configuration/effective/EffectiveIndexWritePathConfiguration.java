package org.hestiastore.index.segmentindex.configuration.effective;

import org.hestiastore.index.Vldtn;

/**
 * Resolved direct-to-segment write-path settings.
 */
public final class EffectiveIndexWritePathConfiguration {

    private final int segmentWriteCacheKeyLimit;
    private final int segmentWriteCacheKeyLimitDuringMaintenance;
    private final int indexBufferedWriteKeyLimit;
    private final int segmentSplitKeyThreshold;

    public EffectiveIndexWritePathConfiguration(
            final int segmentWriteCacheKeyLimit,
            final int segmentWriteCacheKeyLimitDuringMaintenance,
            final int indexBufferedWriteKeyLimit,
            final int segmentSplitKeyThreshold) {
        this.segmentWriteCacheKeyLimit = Vldtn.requireGreaterThanZero(
                segmentWriteCacheKeyLimit, "segmentWriteCacheKeyLimit");
        this.segmentWriteCacheKeyLimitDuringMaintenance = Vldtn
                .requireGreaterThanZero(
                        segmentWriteCacheKeyLimitDuringMaintenance,
                        "segmentWriteCacheKeyLimitDuringMaintenance");
        this.indexBufferedWriteKeyLimit = Vldtn.requireGreaterThanZero(
                indexBufferedWriteKeyLimit, "indexBufferedWriteKeyLimit");
        this.segmentSplitKeyThreshold = Vldtn.requireGreaterThanZero(
                segmentSplitKeyThreshold, "segmentSplitKeyThreshold");
        Vldtn.requireTrue(segmentWriteCacheKeyLimitDuringMaintenance
                > segmentWriteCacheKeyLimit,
                "segmentWriteCacheKeyLimitDuringMaintenance must be greater than segmentWriteCacheKeyLimit");
        Vldtn.requireTrue(indexBufferedWriteKeyLimit
                >= segmentWriteCacheKeyLimitDuringMaintenance,
                "indexBufferedWriteKeyLimit must be greater than or equal to segmentWriteCacheKeyLimitDuringMaintenance");
    }

    public int segmentWriteCacheKeyLimit() {
        return segmentWriteCacheKeyLimit;
    }

    public int segmentWriteCacheKeyLimitDuringMaintenance() {
        return segmentWriteCacheKeyLimitDuringMaintenance;
    }

    public int indexBufferedWriteKeyLimit() {
        return indexBufferedWriteKeyLimit;
    }

    public int segmentSplitKeyThreshold() {
        return segmentSplitKeyThreshold;
    }
}
