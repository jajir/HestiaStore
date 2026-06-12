package org.hestiastore.index.segmentindex.runtimemonitoring.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class SegmentIndexWritePathMetricsTest {

    @Test
    void storesCanonicalWritePathMetrics() {
        final SegmentIndexWritePathMetrics metrics =
                new SegmentIndexWritePathMetrics(10, 14, 42, 9L);

        assertEquals(10, metrics.segmentWriteCacheKeyLimit());
        assertEquals(14, metrics.segmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(42, metrics.indexBufferedWriteKeyLimit());
        assertEquals(9L, metrics.totalBufferedWriteKeys());
    }

    @Test
    void rejectsIndexBufferedLimitBelowMaintenanceLimit() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentIndexWritePathMetrics(10, 14, 13, 0L));

        assertEquals(
                "indexBufferedWriteKeyLimit must be greater than or equal to segmentWriteCacheKeyLimitDuringMaintenance",
                ex.getMessage());
    }
}
