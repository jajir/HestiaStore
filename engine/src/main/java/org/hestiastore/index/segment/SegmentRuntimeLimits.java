package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

/**
 * Runtime-only segment cache/write thresholds.
 *
 * @param maxNumberOfKeysInSegmentCache segment-cache threshold
 * @param maxNumberOfKeysInSegmentWriteCache write-cache threshold
 * @param maxNumberOfKeysInSegmentWriteCacheDuringMaintenance write-cache
 *        threshold allowed while maintenance is running
 */
public record SegmentRuntimeLimits(
        int maxNumberOfKeysInSegmentCache,
        int maxNumberOfKeysInSegmentWriteCache,
        int maxNumberOfKeysInSegmentWriteCacheDuringMaintenance) {

    /**
     * Creates validated runtime limits.
     */
    public SegmentRuntimeLimits {
        Vldtn.requireGreaterThanZero(maxNumberOfKeysInSegmentCache,
                "maxNumberOfKeysInSegmentCache");
        Vldtn.requireGreaterThanZero(maxNumberOfKeysInSegmentWriteCache,
                "maxNumberOfKeysInSegmentWriteCache");
        Vldtn.requireGreaterThanZero(
                maxNumberOfKeysInSegmentWriteCacheDuringMaintenance,
                "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance");
        if (maxNumberOfKeysInSegmentWriteCacheDuringMaintenance
                <= maxNumberOfKeysInSegmentWriteCache) {
            throw new IllegalArgumentException(
                    "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance must be greater than maxNumberOfKeysInSegmentWriteCache");
        }
    }
}
