package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class LegacyPartitionCompatibilityMetricsTest {

    @Test
    void exposesLegacyMetricsFromCanonicalWritePathMetrics() {
        final LegacyPartitionCompatibilityMetrics metrics = new LegacyPartitionCompatibilityMetrics(
                new SegmentIndexWritePathMetrics(10, 14, 42, 9L, 1L, 2L, 3L, 4L,
                        5L, 6L, 7L),
                2, 8, 3, 1, 4, 5, 6L, 7L, 8L, 9, 10L, 11, 12L, 13L);

        assertEquals(10, metrics.getMaxNumberOfKeysInActivePartition());
        assertEquals(14, metrics.getMaxNumberOfKeysInPartitionBuffer());
        assertEquals(42, metrics.getMaxNumberOfKeysInIndexBuffer());
        assertEquals(2, metrics.getMaxNumberOfImmutableRunsPerPartition());
        assertEquals(8, metrics.getPartitionCount());
        assertEquals(13L, metrics.getBufferFullWhileSplitBlockedCount());
    }

    @Test
    void rejectsNegativePartitionCount() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new LegacyPartitionCompatibilityMetrics(
                        new SegmentIndexWritePathMetrics(10, 14, 42, 0L, 0L, 0L,
                                0L, 0L, 0L, 0L, 0L),
                        2, -1, 0, 0, 0, 0, 0L, 0L, 0L, 0, 0L, 0, 0L, 0L));

        assertEquals("partitionCount must be >= 0", ex.getMessage());
    }
}
