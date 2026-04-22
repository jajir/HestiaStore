package org.hestiastore.index.segmentindex;

/**
 * Canonical write-path metrics for the direct-to-segment runtime.
 */
public final class SegmentIndexWritePathMetrics {

    private final int segmentWriteCacheKeyLimit;
    private final int segmentWriteCacheKeyLimitDuringMaintenance;
    private final int indexBufferedWriteKeyLimit;
    private final long totalBufferedWriteKeys;
    private final long putBusyRetryCount;
    private final long putBusyTimeoutCount;
    private final long putBusyWaitP95Micros;
    private final long flushAcceptedToReadyP95Micros;
    private final long compactAcceptedToReadyP95Micros;
    private final long flushBusyRetryCount;
    private final long compactBusyRetryCount;

    /**
     * Creates immutable write-path metrics.
     */
    public SegmentIndexWritePathMetrics(final int segmentWriteCacheKeyLimit,
            final int segmentWriteCacheKeyLimitDuringMaintenance,
            final int indexBufferedWriteKeyLimit,
            final long totalBufferedWriteKeys, final long putBusyRetryCount,
            final long putBusyTimeoutCount, final long putBusyWaitP95Micros,
            final long flushAcceptedToReadyP95Micros,
            final long compactAcceptedToReadyP95Micros,
            final long flushBusyRetryCount,
            final long compactBusyRetryCount) {
        requireNotNegative(segmentWriteCacheKeyLimit,
                "segmentWriteCacheKeyLimit");
        requireNotNegative(segmentWriteCacheKeyLimitDuringMaintenance,
                "segmentWriteCacheKeyLimitDuringMaintenance");
        requireNotNegative(indexBufferedWriteKeyLimit,
                "indexBufferedWriteKeyLimit");
        requireNotNegative(totalBufferedWriteKeys, "totalBufferedWriteKeys");
        requireNotNegative(putBusyRetryCount, "putBusyRetryCount");
        requireNotNegative(putBusyTimeoutCount, "putBusyTimeoutCount");
        requireNotNegative(putBusyWaitP95Micros, "putBusyWaitP95Micros");
        requireNotNegative(flushAcceptedToReadyP95Micros,
                "flushAcceptedToReadyP95Micros");
        requireNotNegative(compactAcceptedToReadyP95Micros,
                "compactAcceptedToReadyP95Micros");
        requireNotNegative(flushBusyRetryCount, "flushBusyRetryCount");
        requireNotNegative(compactBusyRetryCount, "compactBusyRetryCount");
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
        this.segmentWriteCacheKeyLimit = segmentWriteCacheKeyLimit;
        this.segmentWriteCacheKeyLimitDuringMaintenance = segmentWriteCacheKeyLimitDuringMaintenance;
        this.indexBufferedWriteKeyLimit = indexBufferedWriteKeyLimit;
        this.totalBufferedWriteKeys = totalBufferedWriteKeys;
        this.putBusyRetryCount = putBusyRetryCount;
        this.putBusyTimeoutCount = putBusyTimeoutCount;
        this.putBusyWaitP95Micros = putBusyWaitP95Micros;
        this.flushAcceptedToReadyP95Micros = flushAcceptedToReadyP95Micros;
        this.compactAcceptedToReadyP95Micros = compactAcceptedToReadyP95Micros;
        this.flushBusyRetryCount = flushBusyRetryCount;
        this.compactBusyRetryCount = compactBusyRetryCount;
    }

    public int getSegmentWriteCacheKeyLimit() {
        return segmentWriteCacheKeyLimit;
    }

    public int getSegmentWriteCacheKeyLimitDuringMaintenance() {
        return segmentWriteCacheKeyLimitDuringMaintenance;
    }

    public int getIndexBufferedWriteKeyLimit() {
        return indexBufferedWriteKeyLimit;
    }

    public long getTotalBufferedWriteKeys() {
        return totalBufferedWriteKeys;
    }

    public long getPutBusyRetryCount() {
        return putBusyRetryCount;
    }

    public long getPutBusyTimeoutCount() {
        return putBusyTimeoutCount;
    }

    public long getPutBusyWaitP95Micros() {
        return putBusyWaitP95Micros;
    }

    public long getFlushAcceptedToReadyP95Micros() {
        return flushAcceptedToReadyP95Micros;
    }

    public long getCompactAcceptedToReadyP95Micros() {
        return compactAcceptedToReadyP95Micros;
    }

    public long getFlushBusyRetryCount() {
        return flushBusyRetryCount;
    }

    public long getCompactBusyRetryCount() {
        return compactBusyRetryCount;
    }

    private static void requireNotNegative(final long value,
            final String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }
}
