package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

class SegmentIndexMetricsSnapshotTest {

    private static final List<SegmentIndexMetricsSnapshot.SegmentMetricsSnapshot> NO_SEGMENTS = List
            .of();

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
        assertEquals(false, snapshot.isWalEnabled());
        assertEquals(0L, snapshot.getWalAppendCount());
        assertEquals(0L, snapshot.getWalAppendBytes());
        assertEquals(0L, snapshot.getWalSyncCount());
        assertEquals(0L, snapshot.getWalSyncFailureCount());
        assertEquals(0L, snapshot.getWalCorruptionCount());
        assertEquals(0L, snapshot.getWalTruncationCount());
        assertEquals(0L, snapshot.getWalRetainedBytes());
        assertEquals(0, snapshot.getWalSegmentCount());
        assertEquals(0L, snapshot.getWalDurableLsn());
        assertEquals(0L, snapshot.getWalCheckpointLsn());
        assertEquals(0L, snapshot.getWalPendingSyncBytes());
        assertEquals(0L, snapshot.getWalAppliedLsn());
        assertEquals(0L, snapshot.getWalCheckpointLagLsn());
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
    void fullConstructorStoresWalMetricsValues() {
        final SegmentIndexMetricsSnapshot snapshot = new SegmentIndexMetricsSnapshot(
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L,
                0L, 0, 0, 0D, 0L, 0L, 0L, 0L, true, 10L, 100L, 20L, 1L, 2L, 3L,
                4L, 5, 6L, 7L, 8L, List.of(), SegmentIndexState.READY);

        assertTrue(snapshot.isWalEnabled());
        assertEquals(10L, snapshot.getWalAppendCount());
        assertEquals(100L, snapshot.getWalAppendBytes());
        assertEquals(20L, snapshot.getWalSyncCount());
        assertEquals(1L, snapshot.getWalSyncFailureCount());
        assertEquals(2L, snapshot.getWalCorruptionCount());
        assertEquals(3L, snapshot.getWalTruncationCount());
        assertEquals(4L, snapshot.getWalRetainedBytes());
        assertEquals(5, snapshot.getWalSegmentCount());
        assertEquals(6L, snapshot.getWalDurableLsn());
        assertEquals(7L, snapshot.getWalCheckpointLsn());
        assertEquals(8L, snapshot.getWalPendingSyncBytes());
        assertEquals(6L, snapshot.getWalAppliedLsn());
        assertEquals(0L, snapshot.getWalCheckpointLagLsn());
    }

    @Test
    void checkpointLagUsesAppliedLsnWhenProvided() {
        final SegmentIndexMetricsSnapshot snapshot = new SegmentIndexMetricsSnapshot(
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L,
                0L, 0, 0, 0D, 0L, 0L, 0L, 0L, true, 10L, 100L, 20L, 1L, 2L, 3L,
                4L, 5, 6L, 7L, 8L, 11L, List.of(), SegmentIndexState.READY);

        assertEquals(11L, snapshot.getWalAppliedLsn());
        assertEquals(4L, snapshot.getWalCheckpointLagLsn());
    }

    @Test
    void rejectsNegativeGetCount() {
        assertThrows(IllegalArgumentException.class,
                () -> buildSnapshot(-1L, 0L, 0L));
    }

    @Test
    void rejectsNegativePutCount() {
        assertThrows(IllegalArgumentException.class,
                () -> buildSnapshot(0L, -1L, 0L));
    }

    @Test
    void rejectsNegativeDeleteCount() {
        assertThrows(IllegalArgumentException.class,
                () -> buildSnapshot(0L, 0L, -1L));
    }

    private static SegmentIndexMetricsSnapshot buildSnapshot(
            final long getCount, final long putCount,
            final long deleteCount) {
        return new SegmentIndexMetricsSnapshot(getCount, putCount, deleteCount,
                0L, 0L, 0L, 0L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0L, 0L, 0L,
                0L, 0L, 0L, 0L, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0,
                0D, 0L, 0L, 0L, 0L, NO_SEGMENTS, SegmentIndexState.READY);
    }
}
