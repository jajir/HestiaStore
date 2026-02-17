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
        assertEquals(SegmentIndexState.READY, snapshot.getState());
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
