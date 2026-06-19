package org.hestiastore.index.segmentindex.monitoring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentRuntimeSnapshot;
import org.hestiastore.index.segment.SegmentState;
import org.junit.jupiter.api.Test;

class SegmentRuntimeMetricsTest {

    @Test
    void setTotalMappedStableSegmentCountRejectsNegativeValue() {
        final SegmentRuntimeMetrics metrics =
                new SegmentRuntimeMetrics();

        final IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class,
                        () -> metrics.setTotalMappedStableSegmentCount(-1));

        assertEquals(
                "Property 'totalMappedStableSegmentCount' must be greater than or equal to 0",
                error.getMessage());
    }

    @Test
    void setUnloadedMappedStableSegmentCountRejectsNegativeValue() {
        final SegmentRuntimeMetrics metrics =
                new SegmentRuntimeMetrics();

        final IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class,
                        () -> metrics.setUnloadedMappedStableSegmentCount(-1));

        assertEquals(
                "Property 'unloadedMappedStableSegmentCount' must be greater than or equal to 0",
                error.getMessage());
    }

    @Test
    void addSegmentRuntimeSnapshotAggregatesValidatedValues() {
        final SegmentRuntimeMetrics metrics =
                new SegmentRuntimeMetrics();

        metrics.addSegmentRuntimeSnapshot(new SegmentRuntimeSnapshot(
                SegmentId.of(1), SegmentState.READY, 2L, 3L, 4L, 5L, 6, 7,
                8L, 9L, 10L, 11L, 12L, 13L));

        assertEquals(5L, metrics.getTotalStableSegmentKeyCount());
        assertEquals(5L, metrics.getTotalStableSegmentCacheKeyCount());
        assertEquals(6L, metrics.getTotalStableSegmentWriteBufferKeyCount());
        assertEquals(7L, metrics.getTotalStableSegmentDeltaCacheFileCount());
        assertEquals(8L, metrics.getTotalCompactRequestCount());
        assertEquals(9L, metrics.getTotalFlushRequestCount());
        assertEquals(10L, metrics.getTotalBloomFilterRequestCount());
        assertEquals(11L, metrics.getTotalBloomFilterRefusedCount());
        assertEquals(12L, metrics.getTotalBloomFilterPositiveCount());
        assertEquals(13L, metrics.getTotalBloomFilterFalsePositiveCount());
        assertEquals(1, metrics.getStableSegmentRuntimeSnapshots().size());
    }

    @Test
    void addSegmentRuntimeSnapshotRejectsNegativeDerivedKeyCount() {
        final SegmentRuntimeMetrics metrics =
                new SegmentRuntimeMetrics();
        final SegmentRuntimeSnapshot snapshot = new SegmentRuntimeSnapshot(
                SegmentId.of(1), SegmentState.READY, Long.MAX_VALUE, 1L, 0L,
                0L, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L);

        final IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class,
                        () -> metrics.addSegmentRuntimeSnapshot(snapshot));

        assertEquals("Property 'numberOfKeys' must be greater than or equal to 0",
                error.getMessage());
        assertEquals(0, metrics.getStableSegmentRuntimeSnapshots().size());
    }
}
