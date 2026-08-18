package org.hestiastore.index.segmentindex.monitoring.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class SegmentIndexWritePathMetricsTest {

    @Test
    void storesCanonicalWritePathMetrics() {
        final SegmentIndexWritePathMetrics metrics =
                new SegmentIndexWritePathMetrics(10, 14, 9L);

        assertEquals(10, metrics.segmentWriteCacheKeyLimit());
        assertEquals(14, metrics.segmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(9L, metrics.totalBufferedWriteKeys());
    }

    @Test
    void rejectsMaintenanceLimitBelowWriteCacheLimit() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentIndexWritePathMetrics(10, 9, 0L));

        assertEquals(
                "segmentWriteCacheKeyLimitDuringMaintenance must be greater than or equal to segmentWriteCacheKeyLimit",
                ex.getMessage());
    }
}
