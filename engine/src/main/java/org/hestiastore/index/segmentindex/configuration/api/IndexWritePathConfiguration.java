package org.hestiastore.index.segmentindex.configuration.api;

/**
 * Canonical write-path configuration for the direct-to-segment runtime.
 */
public final class IndexWritePathConfiguration {

    private final Integer segmentWriteCacheKeyLimit;
    private final Integer segmentWriteCacheKeyLimitDuringMaintenance;
    private final Integer segmentSplitKeyThreshold;

    /**
     * Creates immutable write-path configuration.
     *
     * @param segmentWriteCacheKeyLimit max keys accepted into one segment write
     *        cache
     * @param segmentWriteCacheKeyLimitDuringMaintenance max buffered keys
     *        allowed while maintenance is running
     * @param segmentSplitKeyThreshold split threshold per routed segment
     */
    public IndexWritePathConfiguration(final Integer segmentWriteCacheKeyLimit,
            final Integer segmentWriteCacheKeyLimitDuringMaintenance,
            final Integer segmentSplitKeyThreshold) {
        requirePositiveIfPresent(segmentWriteCacheKeyLimit,
                "segmentWriteCacheKeyLimit");
        requirePositiveIfPresent(segmentWriteCacheKeyLimitDuringMaintenance,
                "segmentWriteCacheKeyLimitDuringMaintenance");
        requirePositiveIfPresent(segmentSplitKeyThreshold,
                "segmentSplitKeyThreshold");
        if (segmentWriteCacheKeyLimit != null
                && segmentWriteCacheKeyLimitDuringMaintenance != null
                && segmentWriteCacheKeyLimitDuringMaintenance.intValue() <= segmentWriteCacheKeyLimit
                        .intValue()) {
            throw new IllegalArgumentException(
                    "segmentWriteCacheKeyLimitDuringMaintenance must be greater than segmentWriteCacheKeyLimit");
        }
        this.segmentWriteCacheKeyLimit = segmentWriteCacheKeyLimit;
        this.segmentWriteCacheKeyLimitDuringMaintenance = segmentWriteCacheKeyLimitDuringMaintenance;
        this.segmentSplitKeyThreshold = segmentSplitKeyThreshold;
    }

    public Integer segmentWriteCacheKeyLimit() {
        return segmentWriteCacheKeyLimit;
    }

    public Integer segmentWriteCacheKeyLimitDuringMaintenance() {
        return segmentWriteCacheKeyLimitDuringMaintenance;
    }

    public Integer segmentSplitKeyThreshold() {
        return segmentSplitKeyThreshold;
    }

    private static void requirePositiveIfPresent(final Integer value,
            final String name) {
        if (value != null && value.intValue() < 1) {
            throw new IllegalArgumentException(name + " must be >= 1");
        }
    }
}
