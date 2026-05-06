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
    void constructorStoresCanonicalValues() {
        final SegmentIndexMetricsSnapshot snapshot = buildSnapshot(1L, 2L, 3L);

        assertEquals(1L, snapshot.getGetOperationCount());
        assertEquals(2L, snapshot.getPutOperationCount());
        assertEquals(3L, snapshot.getDeleteOperationCount());
        assertEquals(4L, snapshot.getRegistryCacheHitCount());
        assertEquals(5L, snapshot.getRegistryCacheMissCount());
        assertEquals(6L, snapshot.getRegistryCacheLoadCount());
        assertEquals(7L, snapshot.getRegistryCacheEvictionCount());
        assertEquals(8, snapshot.getRegistryCacheSize());
        assertEquals(9, snapshot.getRegistryCacheLimit());
        assertEquals(10, snapshot.getSegmentCacheKeyLimitPerSegment());
        assertEquals(11, snapshot.getSegmentWriteCacheKeyLimit());
        assertEquals(12,
                snapshot.getSegmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(13, snapshot.getIndexBufferedWriteKeyLimit());
        assertEquals(14, snapshot.getSegmentCount());
        assertEquals(15, snapshot.getSegmentReadyCount());
        assertEquals(16, snapshot.getSegmentMaintenanceCount());
        assertEquals(17, snapshot.getSegmentErrorCount());
        assertEquals(18, snapshot.getSegmentClosedCount());
        assertEquals(19, snapshot.getSegmentBusyCount());
        assertEquals(20L, snapshot.getTotalSegmentKeys());
        assertEquals(21L, snapshot.getTotalSegmentCacheKeys());
        assertEquals(22L, snapshot.getTotalBufferedWriteKeys());
        assertEquals(23L, snapshot.getTotalDeltaCacheFiles());
        assertEquals(24L, snapshot.getCompactRequestCount());
        assertEquals(25L, snapshot.getFlushRequestCount());
        assertEquals(26L, snapshot.getSplitScheduleCount());
        assertEquals(27, snapshot.getSplitInFlightCount());
        assertEquals(28, snapshot.getMaintenanceQueueSize());
        assertEquals(29, snapshot.getMaintenanceQueueCapacity());
        assertEquals(30, snapshot.getSplitQueueSize());
        assertEquals(31, snapshot.getSplitQueueCapacity());
        assertEquals(32, snapshot.getIndexMaintenanceActiveThreadCount());
        assertEquals(33L, snapshot.getIndexMaintenanceCompletedTaskCount());
        assertEquals(34L, snapshot.getIndexMaintenanceRejectedTaskCount());
        assertEquals(35, snapshot.getSplitMaintenanceActiveThreadCount());
        assertEquals(36L, snapshot.getSplitMaintenanceCompletedTaskCount());
        assertEquals(37L, snapshot.getSplitMaintenanceRejectedTaskCount());
        assertEquals(38,
                snapshot.getStableSegmentMaintenanceActiveThreadCount());
        assertEquals(39, snapshot.getStableSegmentMaintenanceQueueSize());
        assertEquals(40, snapshot.getStableSegmentMaintenanceQueueCapacity());
        assertEquals(41L,
                snapshot.getStableSegmentMaintenanceCompletedTaskCount());
        assertEquals(42L,
                snapshot.getStableSegmentMaintenanceCallerRunsCount());
        assertEquals(43L, snapshot.getReadLatencyP50Micros());
        assertEquals(44L, snapshot.getReadLatencyP95Micros());
        assertEquals(45L, snapshot.getReadLatencyP99Micros());
        assertEquals(46L, snapshot.getWriteLatencyP50Micros());
        assertEquals(47L, snapshot.getWriteLatencyP95Micros());
        assertEquals(48L, snapshot.getWriteLatencyP99Micros());
        assertEquals(3, snapshot.getBloomFilterHashFunctions());
        assertEquals(1024, snapshot.getBloomFilterIndexSizeInBytes());
        assertEquals(0.01D, snapshot.getBloomFilterProbabilityOfFalsePositive());
        assertEquals(49L, snapshot.getBloomFilterRequestCount());
        assertEquals(50L, snapshot.getBloomFilterRefusedCount());
        assertEquals(51L, snapshot.getBloomFilterPositiveCount());
        assertEquals(52L, snapshot.getBloomFilterFalsePositiveCount());
        assertTrue(snapshot.isWalEnabled());
        assertEquals(53L, snapshot.getWalAppendCount());
        assertEquals(54L, snapshot.getWalAppendBytes());
        assertEquals(55L, snapshot.getWalSyncCount());
        assertEquals(56L, snapshot.getWalSyncFailureCount());
        assertEquals(57L, snapshot.getWalCorruptionCount());
        assertEquals(58L, snapshot.getWalTruncationCount());
        assertEquals(59L, snapshot.getWalRetainedBytes());
        assertEquals(60, snapshot.getWalSegmentCount());
        assertEquals(61L, snapshot.getWalDurableLsn());
        assertEquals(62L, snapshot.getWalCheckpointLsn());
        assertEquals(63L, snapshot.getWalPendingSyncBytes());
        assertEquals(64L, snapshot.getWalAppliedLsn());
        assertEquals(2L, snapshot.getWalCheckpointLagLsn());
        assertEquals(65L, snapshot.getWalSyncTotalNanos());
        assertEquals(66L, snapshot.getWalSyncMaxNanos());
        assertEquals(67L, snapshot.getWalSyncBatchBytesTotal());
        assertEquals(68L, snapshot.getWalSyncBatchBytesMax());
        assertEquals(69L, snapshot.getSplitTaskStartDelayP95Micros());
        assertEquals(70L, snapshot.getSplitTaskRunLatencyP95Micros());
        assertEquals(71L, snapshot.getDrainTaskStartDelayP95Micros());
        assertEquals(72L, snapshot.getDrainTaskRunLatencyP95Micros());
        assertEquals(73L, snapshot.getPutBusyRetryCount());
        assertEquals(74L, snapshot.getPutBusyTimeoutCount());
        assertEquals(75L, snapshot.getPutBusyWaitP95Micros());
        assertEquals(76L, snapshot.getFlushAcceptedToReadyP95Micros());
        assertEquals(77L, snapshot.getCompactAcceptedToReadyP95Micros());
        assertEquals(78L, snapshot.getFlushBusyRetryCount());
        assertEquals(79L, snapshot.getCompactBusyRetryCount());
        assertTrue(snapshot.getSegmentRuntimeSnapshots().isEmpty());
        assertEquals(SegmentIndexState.READY, snapshot.getState());
    }

    @Test
    void rejectsNegativeGetCount() {
        assertThrows(IllegalArgumentException.class,
                () -> buildSnapshot(-1L, 0L, 0L));
    }

    private static SegmentIndexMetricsSnapshot buildSnapshot(
            final long getCount, final long putCount,
            final long deleteCount) {
        return new SegmentIndexMetricsSnapshot(getCount, putCount, deleteCount,
                4L, 5L, 6L, 7L, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                20L, 21L, 22L, 23L, 24L, 25L, 26L, 27, 28, 29, 30, 31, 32,
                33L, 34L, 35, 36L, 37L, 38, 39, 40, 41L, 42L, 43L, 44L, 45L,
                46L, 47L, 48L, 3, 1024, 0.01D, 49L, 50L, 51L, 52L, true,
                53L, 54L, 55L, 56L, 57L, 58L, 59L, 60, 61L, 62L, 63L, 64L,
                65L, 66L, 67L, 68L, 69L, 70L, 71L, 72L, 73L, 74L, 75L, 76L,
                77L, 78L, 79L, NO_SEGMENTS, SegmentIndexState.READY);
    }
}
