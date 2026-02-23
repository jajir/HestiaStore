package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

class SegmentIndexMetricsSnapshotTest {

    @Test
    void fullConstructorCanRepresentEmptySnapshot() {
        final SegmentIndexMetricsSnapshot snapshot = new SegmentIndexMetricsSnapshot(
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L,
                0L, 0, 0, 0D, 0L, 0L, 0L, 0L, List.of(), SegmentIndexState.READY);

        assertEquals(0L, snapshot.getGetOperationCount());
        assertEquals(0L, snapshot.getPutOperationCount());
        assertEquals(0L, snapshot.getDeleteOperationCount());
        assertEquals(0L, snapshot.getRegistryCacheHitCount());
        assertEquals(0L, snapshot.getRegistryCacheMissCount());
        assertEquals(0L, snapshot.getRegistryCacheLoadCount());
        assertEquals(0L, snapshot.getRegistryCacheEvictionCount());
        assertEquals(0, snapshot.getRegistryCacheSize());
        assertEquals(0, snapshot.getRegistryCacheLimit());
        assertTrue(snapshot.getSegmentRuntimeSnapshots().isEmpty());
        assertEquals(SegmentIndexState.READY, snapshot.getState());
    }

    @Test
    void fullConstructorStoresProvidedValues() {
        final SegmentIndexMetricsSnapshot snapshot = new SegmentIndexMetricsSnapshot(
                3L, 4L, 5L, 11L, 12L, 13L, 14L, 2, 64, 0, 0, 0, 0, 0, 0, 0, 0,
                0,
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L,
                0L, 0, 0, 0D, 0L, 0L, 0L, 0L, List.of(), SegmentIndexState.READY);

        assertEquals(3L, snapshot.getGetOperationCount());
        assertEquals(4L, snapshot.getPutOperationCount());
        assertEquals(5L, snapshot.getDeleteOperationCount());
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
            new SegmentIndexMetricsSnapshot(-1L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0,
                    0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0D, 0L, 0L, 0L,
                    0L, List.of(), SegmentIndexState.READY);
        });
    }

    @Test
    void rejectsNegativePutCount() {
        assertThrows(IllegalArgumentException.class, () -> {
            new SegmentIndexMetricsSnapshot(0L, -1L, 0L, 0L, 0L, 0L, 0L, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0,
                    0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0D, 0L, 0L, 0L,
                    0L, List.of(), SegmentIndexState.READY);
        });
    }

    @Test
    void rejectsNegativeDeleteCount() {
        assertThrows(IllegalArgumentException.class, () -> {
            new SegmentIndexMetricsSnapshot(0L, 0L, -1L, 0L, 0L, 0L, 0L, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0,
                    0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0D, 0L, 0L, 0L,
                    0L, List.of(), SegmentIndexState.READY);
        });
    }
}
