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

        assertZeroOperationAndCacheMetrics(snapshot);
        assertZeroWalMetrics(snapshot);
        assertZeroPartitionMetrics(snapshot);
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
        assertEquals(0, snapshot.getSegmentWriteCacheKeyLimit());
        assertEquals(0, snapshot.getSegmentWriteCacheKeyLimitDuringMaintenance());
    }

    @Test
    void fullConstructorStoresPartitionMetricsValues() {
        final SegmentIndexMetricsSnapshot snapshot = new SegmentIndexMetricsSnapshot(
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, 11, 12, 0, 0, 0, 0, 0,
                0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L,
                0L, 0L, 0, 0, 0D, 0L, 0L, 0L, 0L, 17, 18, 19, 20, 21, 22, 23,
                24L, 25L, 26L, 27, List.of(),
                SegmentIndexState.READY);

        assertEquals(11, snapshot.getLegacyPartitionCompatibilityMetrics().getMaxNumberOfKeysInActivePartition());
        assertEquals(12, snapshot.getLegacyPartitionCompatibilityMetrics().getMaxNumberOfKeysInPartitionBuffer());
        assertEquals(17, snapshot.getLegacyPartitionCompatibilityMetrics().getMaxNumberOfImmutableRunsPerPartition());
        assertEquals(18, snapshot.getLegacyPartitionCompatibilityMetrics().getMaxNumberOfKeysInIndexBuffer());
        assertEquals(11, snapshot.getSegmentWriteCacheKeyLimit());
        assertEquals(12, snapshot.getSegmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(18, snapshot.getIndexBufferedWriteKeyLimit());
        assertEquals(19, snapshot.getLegacyPartitionCompatibilityMetrics().getPartitionCount());
        assertEquals(20, snapshot.getLegacyPartitionCompatibilityMetrics().getActivePartitionCount());
        assertEquals(21, snapshot.getLegacyPartitionCompatibilityMetrics().getDrainingPartitionCount());
        assertEquals(22, snapshot.getLegacyPartitionCompatibilityMetrics().getImmutableRunCount());
        assertEquals(23, snapshot.getLegacyPartitionCompatibilityMetrics().getPartitionBufferedKeyCount());
        assertEquals(24L, snapshot.getLegacyPartitionCompatibilityMetrics().getLocalThrottleCount());
        assertEquals(25L, snapshot.getLegacyPartitionCompatibilityMetrics().getGlobalThrottleCount());
        assertEquals(26L, snapshot.getLegacyPartitionCompatibilityMetrics().getDrainScheduleCount());
        assertEquals(27, snapshot.getLegacyPartitionCompatibilityMetrics().getDrainInFlightCount());
        assertEquals(0L, snapshot.getLegacyPartitionCompatibilityMetrics().getDrainLatencyP95Micros());
    }

    @Test
    void fullConstructorStoresExecutorMetricsValues() {
        final SegmentIndexMetricsSnapshot snapshot = new SegmentIndexMetricsSnapshot(
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0L, 0L, 0L, 0L, 0L, 0L, 10L, 11, 12, 13, 14, 15, 16, 17L, 18L,
                19, 20L, 21L, 22, 23, 24, 25L, 26L, 0L, 0L, 0L, 0L, 0L, 0L, 0,
                0, 0D, 0L, 0L, 0L, 0L, false, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0,
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, 0, 0, 0, 0, 0L, 0L,
                0L, 0, 0L, NO_SEGMENTS,
                SegmentIndexState.READY);

        assertEquals(10L, snapshot.getSplitScheduleCount());
        assertEquals(11, snapshot.getSplitInFlightCount());
        assertEquals(12, snapshot.getMaintenanceQueueSize());
        assertEquals(13, snapshot.getMaintenanceQueueCapacity());
        assertEquals(14, snapshot.getSplitQueueSize());
        assertEquals(15, snapshot.getSplitQueueCapacity());
        assertEquals(16, snapshot.getIndexMaintenanceActiveThreadCount());
        assertEquals(17L, snapshot.getIndexMaintenanceCompletedTaskCount());
        assertEquals(18L, snapshot.getIndexMaintenanceRejectedTaskCount());
        assertEquals(19, snapshot.getSplitMaintenanceActiveThreadCount());
        assertEquals(20L, snapshot.getSplitMaintenanceCompletedTaskCount());
        assertEquals(21L, snapshot.getSplitMaintenanceRejectedTaskCount());
        assertEquals(22, snapshot.getStableSegmentMaintenanceActiveThreadCount());
        assertEquals(23, snapshot.getStableSegmentMaintenanceQueueSize());
        assertEquals(24, snapshot.getStableSegmentMaintenanceQueueCapacity());
        assertEquals(25L,
                snapshot.getStableSegmentMaintenanceCompletedTaskCount());
        assertEquals(26L,
                snapshot.getStableSegmentMaintenanceCallerRunsCount());
        assertEquals(0L, snapshot.getFlushAcceptedToReadyP95Micros());
        assertEquals(0L, snapshot.getCompactAcceptedToReadyP95Micros());
        assertEquals(0L, snapshot.getFlushBusyRetryCount());
        assertEquals(0L, snapshot.getCompactBusyRetryCount());
    }

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
        assertEquals(0L, snapshot.getWalSyncTotalNanos());
        assertEquals(0L, snapshot.getWalSyncMaxNanos());
        assertEquals(0L, snapshot.getWalSyncBatchBytesTotal());
        assertEquals(0L, snapshot.getWalSyncBatchBytesMax());
    }

    @Test
    void fullConstructorStoresWalSyncMetricsValues() {
        final SegmentIndexMetricsSnapshot snapshot = new SegmentIndexMetricsSnapshot(
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L,
                0L, 0, 0, 0D, 0L, 0L, 0L, 0L, true, 10L, 100L, 4L, 1L, 2L, 3L,
                4L, 5, 6L, 7L, 8L, 11L, 1_000L, 500L, 4_096L, 2_048L, List.of(),
                SegmentIndexState.READY);

        assertEquals(1_000L, snapshot.getWalSyncTotalNanos());
        assertEquals(500L, snapshot.getWalSyncMaxNanos());
        assertEquals(4_096L, snapshot.getWalSyncBatchBytesTotal());
        assertEquals(2_048L, snapshot.getWalSyncBatchBytesMax());
        assertEquals(250L, snapshot.getWalSyncAvgNanos());
        assertEquals(1_024L, snapshot.getWalSyncAvgBatchBytes());
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

    private static void assertZeroOperationAndCacheMetrics(
            final SegmentIndexMetricsSnapshot snapshot) {
        assertEquals(0L, snapshot.getGetOperationCount());
        assertEquals(0L, snapshot.getPutOperationCount());
        assertEquals(0L, snapshot.getDeleteOperationCount());
        assertEquals(0L, snapshot.getRegistryCacheHitCount());
        assertEquals(0L, snapshot.getRegistryCacheMissCount());
        assertEquals(0L, snapshot.getRegistryCacheLoadCount());
        assertEquals(0L, snapshot.getRegistryCacheEvictionCount());
        assertEquals(0, snapshot.getRegistryCacheSize());
        assertEquals(0, snapshot.getRegistryCacheLimit());
    }

    private static void assertZeroWalMetrics(
            final SegmentIndexMetricsSnapshot snapshot) {
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
        assertEquals(0L, snapshot.getWalSyncTotalNanos());
        assertEquals(0L, snapshot.getWalSyncMaxNanos());
        assertEquals(0L, snapshot.getWalSyncBatchBytesTotal());
        assertEquals(0L, snapshot.getWalSyncBatchBytesMax());
        assertEquals(0L, snapshot.getWalSyncAvgNanos());
        assertEquals(0L, snapshot.getWalSyncAvgBatchBytes());
    }

    private static void assertZeroPartitionMetrics(
            final SegmentIndexMetricsSnapshot snapshot) {
        assertEquals(0, snapshot.getWritePathMetrics().getSegmentWriteCacheKeyLimit());
        assertEquals(0,
                snapshot.getWritePathMetrics()
                        .getSegmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(0,
                snapshot.getWritePathMetrics().getIndexBufferedWriteKeyLimit());
        assertEquals(0, snapshot.getLegacyPartitionCompatibilityMetrics().getMaxNumberOfKeysInActivePartition());
        assertEquals(0, snapshot.getLegacyPartitionCompatibilityMetrics().getMaxNumberOfImmutableRunsPerPartition());
        assertEquals(0, snapshot.getLegacyPartitionCompatibilityMetrics().getMaxNumberOfKeysInPartitionBuffer());
        assertEquals(0, snapshot.getLegacyPartitionCompatibilityMetrics().getMaxNumberOfKeysInIndexBuffer());
        assertEquals(0, snapshot.getLegacyPartitionCompatibilityMetrics().getPartitionCount());
        assertEquals(0, snapshot.getLegacyPartitionCompatibilityMetrics().getActivePartitionCount());
        assertEquals(0, snapshot.getLegacyPartitionCompatibilityMetrics().getDrainingPartitionCount());
        assertEquals(0, snapshot.getLegacyPartitionCompatibilityMetrics().getImmutableRunCount());
        assertEquals(0, snapshot.getLegacyPartitionCompatibilityMetrics().getPartitionBufferedKeyCount());
        assertEquals(0L, snapshot.getLegacyPartitionCompatibilityMetrics().getLocalThrottleCount());
        assertEquals(0L, snapshot.getLegacyPartitionCompatibilityMetrics().getGlobalThrottleCount());
        assertEquals(0L, snapshot.getLegacyPartitionCompatibilityMetrics().getDrainScheduleCount());
        assertEquals(0, snapshot.getLegacyPartitionCompatibilityMetrics().getDrainInFlightCount());
        assertEquals(0L, snapshot.getLegacyPartitionCompatibilityMetrics().getDrainLatencyP95Micros());
        assertEquals(0, snapshot.getLegacyPartitionCompatibilityMetrics().getSplitBlockedPartitionCount());
        assertEquals(0L, snapshot.getLegacyPartitionCompatibilityMetrics().getSplitBlockedDrainScheduleCount());
        assertEquals(0L, snapshot.getLegacyPartitionCompatibilityMetrics().getBufferFullWhileSplitBlockedCount());
        assertEquals(0L, snapshot.getPutBusyRetryCount());
        assertEquals(0L, snapshot.getPutBusyTimeoutCount());
        assertEquals(0L, snapshot.getPutBusyWaitP95Micros());
        assertEquals(0L, snapshot.getFlushAcceptedToReadyP95Micros());
        assertEquals(0L, snapshot.getCompactAcceptedToReadyP95Micros());
        assertEquals(0L, snapshot.getFlushBusyRetryCount());
        assertEquals(0L, snapshot.getCompactBusyRetryCount());
    }
}
