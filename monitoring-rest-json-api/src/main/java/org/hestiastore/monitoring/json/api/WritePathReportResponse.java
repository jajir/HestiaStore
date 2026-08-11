package org.hestiastore.monitoring.json.api;

import java.beans.ConstructorProperties;

/**
 * Write-path metrics section inside an index report payload.
 */
@SuppressWarnings("java:S6206")
public final class WritePathReportResponse {

    private final int segmentWriteCacheKeyLimit;
    private final int segmentWriteCacheKeyLimitDuringMaintenance;
    private final long totalBufferedWriteKeys;

    /**
     * Creates write-path metrics.
     *
     * @param segmentWriteCacheKeyLimit segment write-cache key limit
     * @param segmentWriteCacheKeyLimitDuringMaintenance maintenance-time write
     *        cache key limit
     * @param totalBufferedWriteKeys total buffered write keys
     */
    @ConstructorProperties({ "segmentWriteCacheKeyLimit",
            "segmentWriteCacheKeyLimitDuringMaintenance",
            "totalBufferedWriteKeys" })
    public WritePathReportResponse(final int segmentWriteCacheKeyLimit,
            final int segmentWriteCacheKeyLimitDuringMaintenance,
            final long totalBufferedWriteKeys) {
        this.segmentWriteCacheKeyLimit = segmentWriteCacheKeyLimit;
        this.segmentWriteCacheKeyLimitDuringMaintenance =
                segmentWriteCacheKeyLimitDuringMaintenance;
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
     * Returns total buffered write keys.
     *
     * @return total buffered write keys
     */
    public long totalBufferedWriteKeys() {
        return totalBufferedWriteKeys;
    }
}
