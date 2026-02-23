package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class SegmentRuntimeSnapshotTest {

    @Test
    void gettersExposeConfiguredValues() {
        final SegmentRuntimeSnapshot snapshot = new SegmentRuntimeSnapshot(
                SegmentId.of(7), SegmentState.READY,
                3L, 4L, 2L, 5L, 6, 7, 8L, 9L,
                10L, 11L, 12L, 13L);

        assertEquals(SegmentId.of(7), snapshot.getSegmentId());
        assertEquals(SegmentState.READY, snapshot.getState());
        assertEquals(3L, snapshot.getNumberOfKeysInDeltaCache());
        assertEquals(4L, snapshot.getNumberOfKeysInSegment());
        assertEquals(7L, snapshot.getNumberOfKeys());
        assertEquals(2L, snapshot.getNumberOfKeysInScarceIndex());
        assertEquals(5L, snapshot.getNumberOfKeysInSegmentCache());
        assertEquals(6, snapshot.getNumberOfKeysInWriteCache());
        assertEquals(7, snapshot.getNumberOfDeltaCacheFiles());
        assertEquals(8L, snapshot.getNumberOfCompacts());
        assertEquals(9L, snapshot.getNumberOfFlushes());
        assertEquals(10L, snapshot.getBloomFilterRequestCount());
        assertEquals(11L, snapshot.getBloomFilterRefusedCount());
        assertEquals(12L, snapshot.getBloomFilterPositiveCount());
        assertEquals(13L, snapshot.getBloomFilterFalsePositiveCount());
    }

    @Test
    void rejectsNegativeValues() {
        final SegmentId segmentId = SegmentId.of(1);
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentRuntimeSnapshot(segmentId,
                        SegmentState.READY, -1L, 0L, 0L, 0L, 0, 0, 0L, 0L,
                        0L, 0L, 0L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentRuntimeSnapshot(segmentId,
                        SegmentState.READY, 0L, 0L, 0L, 0L, 0, 0, -1L, 0L,
                        0L, 0L, 0L, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentRuntimeSnapshot(segmentId,
                        SegmentState.READY, 0L, 0L, 0L, 0L, 0, 0, 0L, -1L,
                        0L, 0L, 0L, 0L));
    }
}
