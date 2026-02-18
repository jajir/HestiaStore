package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class SegmentIndexMetricsSnapshotTest {

    @Test
    void storesProvidedValues() {
        final SegmentIndexMetricsSnapshot snapshot = new SegmentIndexMetricsSnapshot(
                3L, 4L, 5L, SegmentIndexState.READY);

        assertEquals(3L, snapshot.getGetOperationCount());
        assertEquals(4L, snapshot.getPutOperationCount());
        assertEquals(5L, snapshot.getDeleteOperationCount());
        assertEquals(0L, snapshot.getRegistryCacheHitCount());
        assertEquals(0L, snapshot.getRegistryCacheMissCount());
        assertEquals(0L, snapshot.getRegistryCacheLoadCount());
        assertEquals(0L, snapshot.getRegistryCacheEvictionCount());
        assertEquals(0, snapshot.getRegistryCacheSize());
        assertEquals(0, snapshot.getRegistryCacheLimit());
        assertEquals(SegmentIndexState.READY, snapshot.getState());
    }

    @Test
    void storesProvidedCacheValues() {
        final SegmentIndexMetricsSnapshot snapshot = new SegmentIndexMetricsSnapshot(
                3L, 4L, 5L, 11L, 12L, 13L, 14L, 2, 64,
                SegmentIndexState.READY);

        assertEquals(11L, snapshot.getRegistryCacheHitCount());
        assertEquals(12L, snapshot.getRegistryCacheMissCount());
        assertEquals(13L, snapshot.getRegistryCacheLoadCount());
        assertEquals(14L, snapshot.getRegistryCacheEvictionCount());
        assertEquals(2, snapshot.getRegistryCacheSize());
        assertEquals(64, snapshot.getRegistryCacheLimit());
    }

    @Test
    void rejectsNegativeGetCount() {
        assertThrows(IllegalArgumentException.class, () -> {
            new SegmentIndexMetricsSnapshot(-1L, 0L, 0L,
                    SegmentIndexState.READY);
        });
    }

    @Test
    void rejectsNegativePutCount() {
        assertThrows(IllegalArgumentException.class, () -> {
            new SegmentIndexMetricsSnapshot(0L, -1L, 0L,
                    SegmentIndexState.READY);
        });
    }

    @Test
    void rejectsNegativeDeleteCount() {
        assertThrows(IllegalArgumentException.class, () -> {
            new SegmentIndexMetricsSnapshot(0L, 0L, -1L,
                    SegmentIndexState.READY);
        });
    }
}
