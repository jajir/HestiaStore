package org.hestiastore.monitoring.json.api;

import java.beans.ConstructorProperties;

/**
 * Write-path metrics section inside an index report payload.
 */
@SuppressWarnings("java:S6206")
public final class WritePathReportResponse {

    private final int segmentWriteCacheKeyLimit;
    private final int segmentWriteCacheKeyLimitDuringMaintenance;
    private final int indexBufferedWriteKeyLimit;
    private final long totalBufferedWriteKeys;

    /**
     * Creates write-path metrics.
     *
     * @param segmentWriteCacheKeyLimit segment write-cache key limit
     * @param segmentWriteCacheKeyLimitDuringMaintenance maintenance-time write
     *        cache key limit
     * @param indexBufferedWriteKeyLimit index buffered write key limit
     * @param totalBufferedWriteKeys total buffered write keys
     */
    @ConstructorProperties({ "segmentWriteCacheKeyLimit",
            "segmentWriteCacheKeyLimitDuringMaintenance",
            "indexBufferedWriteKeyLimit", "totalBufferedWriteKeys" })
    public WritePathReportResponse(final int segmentWriteCacheKeyLimit,
            final int segmentWriteCacheKeyLimitDuringMaintenance,
            final int indexBufferedWriteKeyLimit,
            final long totalBufferedWriteKeys) {
        this.segmentWriteCacheKeyLimit = segmentWriteCacheKeyLimit;
        this.segmentWriteCacheKeyLimitDuringMaintenance =
                segmentWriteCacheKeyLimitDuringMaintenance;
        this.indexBufferedWriteKeyLimit = indexBufferedWriteKeyLimit;
        this.totalBufferedWriteKeys = totalBufferedWriteKeys;
    }

    /**
     * Returns segment write-cache key limit.
     *
     * @return segment write-cache key limit
     */
    public int segmentWriteCacheKeyLimit() {
        return segmentWriteCacheKeyLimit;
    }

    /**
     * Returns maintenance-time write-cache key limit.
     *
     * @return maintenance-time write-cache key limit
     */
    public int segmentWriteCacheKeyLimitDuringMaintenance() {
        return segmentWriteCacheKeyLimitDuringMaintenance;
    }

    /**
     * Returns index buffered write key limit.
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
