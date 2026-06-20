package org.hestiastore.index.segmentindex.monitoring.model;

/**
 * User-facing direct write-path metrics.
 */
public final class SegmentIndexWritePathMetrics {

    private final int segmentWriteCacheKeyLimit;
    private final int segmentWriteCacheKeyLimitDuringMaintenance;
    private final int indexBufferedWriteKeyLimit;
    private final long totalBufferedWriteKeys;

    /**
     * Creates write-path metrics.
     *
     * @param segmentWriteCacheKeyLimit normal segment write-cache key limit
     * @param segmentWriteCacheKeyLimitDuringMaintenance maintenance-time
     *        segment write-cache key limit
     * @param indexBufferedWriteKeyLimit index-wide buffered write key limit
     * @param totalBufferedWriteKeys total buffered write keys
     */
    public SegmentIndexWritePathMetrics(final int segmentWriteCacheKeyLimit,
            final int segmentWriteCacheKeyLimitDuringMaintenance,
            final int indexBufferedWriteKeyLimit,
            final long totalBufferedWriteKeys) {
        this.segmentWriteCacheKeyLimit = MetricModelValidation.nonNegative(
                segmentWriteCacheKeyLimit, "segmentWriteCacheKeyLimit");
        this.segmentWriteCacheKeyLimitDuringMaintenance =
                MetricModelValidation.nonNegative(
                        segmentWriteCacheKeyLimitDuringMaintenance,
                        "segmentWriteCacheKeyLimitDuringMaintenance");
        this.indexBufferedWriteKeyLimit = MetricModelValidation.nonNegative(
                indexBufferedWriteKeyLimit, "indexBufferedWriteKeyLimit");
        this.totalBufferedWriteKeys = MetricModelValidation.nonNegative(
                totalBufferedWriteKeys, "totalBufferedWriteKeys");
        if (segmentWriteCacheKeyLimitDuringMaintenance
                < segmentWriteCacheKeyLimit) {
            throw new IllegalArgumentException(
                    "segmentWriteCacheKeyLimitDuringMaintenance must be greater than or equal to segmentWriteCacheKeyLimit");
        }
        if (indexBufferedWriteKeyLimit
                < segmentWriteCacheKeyLimitDuringMaintenance) {
            throw new IllegalArgumentException(
                    "indexBufferedWriteKeyLimit must be greater than or equal to segmentWriteCacheKeyLimitDuringMaintenance");
        }
    }

    /**
     * Returns normal segment write-cache key limit.
     *
     * @return segment write-cache key limit
     */
    public int segmentWriteCacheKeyLimit() {
        return segmentWriteCacheKeyLimit;
    }

    /**
     * Returns maintenance-time segment write-cache key limit.
     *
     * @return maintenance-time segment write-cache key limit
     */
    public int segmentWriteCacheKeyLimitDuringMaintenance() {
        return segmentWriteCacheKeyLimitDuringMaintenance;
    }

    /**
     * Returns index-wide buffered write key limit.
     *
     * @return index buffered write key limit
     */
    public int indexBufferedWriteKeyLimit() {
        return indexBufferedWriteKeyLimit;
    }

    /**
     * Returns total buffered write keys.
     *
     * @return total buffered write keys
     */
    public long totalBufferedWriteKeys() {
        return totalBufferedWriteKeys;
    }
}
