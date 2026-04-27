package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class SegmentIndexWritePathMetricsTest {

    @Test
    void storesCanonicalWritePathMetrics() {
        final SegmentIndexWritePathMetrics metrics = new SegmentIndexWritePathMetrics(
                10, 14, 42, 9L, 1L, 2L, 3L, 4L, 5L, 6L, 7L);

        assertEquals(10, metrics.getSegmentWriteCacheKeyLimit());
        assertEquals(14, metrics.getSegmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(42, metrics.getIndexBufferedWriteKeyLimit());
        assertEquals(9L, metrics.getTotalBufferedWriteKeys());
        assertEquals(1L, metrics.getPutBusyRetryCount());
        assertEquals(7L, metrics.getCompactBusyRetryCount());
    }

    @Test
    void rejectsIndexBufferedLimitBelowMaintenanceLimit() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentIndexWritePathMetrics(10, 14, 13, 0L, 0L, 0L,
                        0L, 0L, 0L, 0L, 0L));

        assertEquals(
                "indexBufferedWriteKeyLimit must be greater than or equal to segmentWriteCacheKeyLimitDuringMaintenance",
                ex.getMessage());
    }
}
