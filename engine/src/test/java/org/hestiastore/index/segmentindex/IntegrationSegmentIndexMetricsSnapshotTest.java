package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.segmentindex.tuning.RuntimeConfigPatch;
import org.hestiastore.index.segmentindex.tuning.RuntimePatchResult;
import org.hestiastore.index.segmentindex.tuning.RuntimeSettingKey;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class IntegrationSegmentIndexMetricsSnapshotTest {

    @Test
    void metricsSnapshotCountsPointOperations() {
        final Directory directory = new MemDirectory();
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(keyDescriptor)) //
                .identity(identity -> identity.valueTypeDescriptor(valueDescriptor)) //
                .segment(segment -> segment.cacheKeyLimit(8)) //
                .segment(segment -> segment.maxKeys(8)) //
                .segment(segment -> segment.chunkKeyLimit(4)) //
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024 * 1024)) //
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(4)) //
                .logging(logging -> logging.contextEnabled(false)) //
                .identity(identity -> identity.name("metrics_test_index")) //
                .build();

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            index.put(1, "a");
            index.put(2, "b");
            assertEquals("a", index.get(1));
            assertNull(index.get(99));
            assertNull(index.get(100));
            assertNull(index.get(101));
            index.delete(2);

            final SegmentIndexMetricsSnapshot snapshot = index.runtimeMonitoring().snapshot().getMetrics();
            assertOperationCounts(snapshot, 2L, 4L, 1L);
            assertDirectWriteBufferMetrics(snapshot);
            assertRegistryAndBloomMetrics(snapshot);
            assertWalDisabledSnapshot(snapshot);
            assertReadySnapshot(snapshot);
        }
    }

    @Test
    void metricsSnapshotExposesExecutorRuntimeMetrics() {
        final Directory directory = new MemDirectory();
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(keyDescriptor)) //
                .identity(identity -> identity.valueTypeDescriptor(valueDescriptor)) //
                .segment(segment -> segment.cacheKeyLimit(8)) //
                .segment(segment -> segment.maxKeys(32)) //
                .segment(segment -> segment.chunkKeyLimit(4)) //
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024 * 1024)) //
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(4)) //
                .logging(logging -> logging.contextEnabled(false)) //
                .identity(identity -> identity.name("metrics_executor_runtime_test_index")) //
                .build();

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            index.put(1, "a");
            final SegmentIndexMetricsSnapshot snapshot = index.runtimeMonitoring().snapshot().getMetrics();

            assertTrue(snapshot.getMaintenanceQueueCapacity() > 0);
            assertTrue(snapshot.getSplitQueueCapacity() > 0);
            assertTrue(snapshot.getStableSegmentMaintenanceQueueCapacity() > 0);
            assertTrue(snapshot.getMaintenanceQueueSize() >= 0);
            assertTrue(snapshot.getSplitQueueSize() >= 0);
            assertTrue(snapshot.getStableSegmentMaintenanceQueueSize() >= 0);
            assertTrue(snapshot.getIndexMaintenanceActiveThreadCount() >= 0);
            assertTrue(snapshot.getSplitMaintenanceActiveThreadCount() >= 0);
            assertTrue(
                    snapshot.getStableSegmentMaintenanceActiveThreadCount() >= 0);
            assertTrue(snapshot.getIndexMaintenanceCompletedTaskCount() >= 0L);
            assertTrue(snapshot.getSplitMaintenanceCompletedTaskCount() >= 0L);
            assertTrue(snapshot
                    .getStableSegmentMaintenanceCompletedTaskCount() >= 0L);
            assertTrue(snapshot.getIndexMaintenanceRejectedTaskCount() >= 0L);
            assertTrue(snapshot.getSplitMaintenanceRejectedTaskCount() >= 0L);
            assertTrue(
                    snapshot.getStableSegmentMaintenanceCallerRunsCount() >= 0L);
            assertTrue(snapshot.getSplitTaskStartDelayP95Micros() >= 0L);
            assertTrue(snapshot.getSplitTaskRunLatencyP95Micros() >= 0L);
            assertTrue(snapshot.getDrainTaskStartDelayP95Micros() >= 0L);
            assertTrue(snapshot.getDrainTaskRunLatencyP95Micros() >= 0L);
            assertTrue(snapshot.getPutBusyRetryCount() >= 0L);
            assertTrue(snapshot.getPutBusyTimeoutCount() >= 0L);
            assertTrue(snapshot.getPutBusyWaitP95Micros() >= 0L);
            assertTrue(snapshot.getFlushAcceptedToReadyP95Micros() >= 0L);
            assertTrue(snapshot.getCompactAcceptedToReadyP95Micros() >= 0L);
            assertTrue(snapshot.getFlushBusyRetryCount() >= 0L);
            assertTrue(snapshot.getCompactBusyRetryCount() >= 0L);
        }
    }

    @Test
    void metricsSnapshotExposesWalStatsWhenEnabled() {
        final Directory directory = new MemDirectory();
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(keyDescriptor)) //
                .identity(identity -> identity.valueTypeDescriptor(valueDescriptor)) //
                .segment(segment -> segment.maxKeys(64)) //
                .identity(identity -> identity.name("metrics_wal_enabled_test_index")) //
                .wal(wal -> wal.configuration(IndexWalConfiguration.builder().build())) //
                .build();

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            index.put(1, "a");
            index.put(2, "b");
            index.delete(1);
            index.maintenance().flushAndWait();

            final SegmentIndexMetricsSnapshot snapshot = index.runtimeMonitoring().snapshot().getMetrics();
            assertTrue(snapshot.isWalEnabled());
            assertTrue(snapshot.getWalAppendCount() >= 3L);
            assertTrue(snapshot.getWalAppendBytes() > 0L);
            assertTrue(snapshot.getWalSyncCount() >= 1L);
            assertEquals(0L, snapshot.getWalSyncFailureCount());
            assertEquals(0L, snapshot.getWalCorruptionCount());
            assertEquals(0L, snapshot.getWalTruncationCount());
            assertTrue(snapshot.getWalSegmentCount() >= 1);
            assertTrue(snapshot.getWalDurableLsn() >= 3L);
            assertTrue(snapshot.getWalCheckpointLsn() >= 3L);
            assertTrue(snapshot.getWalAppliedLsn() >= 3L);
            assertTrue(snapshot.getWalCheckpointLagLsn() >= 0L);
            assertTrue(snapshot.getWalSyncTotalNanos() >= 0L);
            assertTrue(snapshot.getWalSyncMaxNanos() >= 0L);
            assertTrue(snapshot.getWalSyncBatchBytesTotal() > 0L);
            assertTrue(snapshot.getWalSyncBatchBytesMax() > 0L);
            assertTrue(snapshot.getWalSyncBatchBytesTotal() >= snapshot
                    .getWalSyncBatchBytesMax());
            assertTrue(snapshot.getWalSyncAvgNanos() >= 0L);
            assertTrue(snapshot.getWalSyncAvgBatchBytes() > 0L);
        }
    }

    @Test
    void writePathMetricsTrackDirectSegmentWrites() {
        final Directory directory = new MemDirectory();
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(keyDescriptor)) //
                .identity(identity -> identity.valueTypeDescriptor(valueDescriptor)) //
                .segment(segment -> segment.cacheKeyLimit(5)) //
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(8)) //
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(16)) //
                .writePath(writePath -> writePath.indexBufferedWriteKeyLimit(64)) //
                .segment(segment -> segment.maxKeys(512)) //
                .segment(segment -> segment.chunkKeyLimit(4)) //
                .segment(segment -> segment.deltaCacheFileLimit(1)) //
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024 * 128)) //
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(3)) //
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(true)) //
                .logging(logging -> logging.contextEnabled(false)) //
                .identity(identity -> identity.name("metrics_compaction_monotonicity_test")) //
                .build();

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            for (int i = 0; i < 128; i++) {
                index.put(i, "base-" + i);
            }

            awaitIdle(index);
            final SegmentIndexMetricsSnapshot beforeRotation = index.runtimeMonitoring().snapshot().getMetrics();
            assertEquals(0, beforeRotation.getSplitInFlightCount());

            for (int i = 128; i < 256; i++) {
                index.put(i, "next-" + i);
            }
            awaitIdle(index);

            final SegmentIndexMetricsSnapshot afterRotation = index.runtimeMonitoring().snapshot().getMetrics();
            assertEquals(0, afterRotation.getSplitInFlightCount());
        }
    }

    @Test
    void metricsSnapshot_doesNotLoadSegmentsOutsideRegistryCache() {
        final Directory directory = new MemDirectory();
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(keyDescriptor)) //
                .identity(identity -> identity.valueTypeDescriptor(valueDescriptor)) //
                .segment(segment -> segment.cacheKeyLimit(5)) //
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(128)) //
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(192)) //
                .writePath(writePath -> writePath.indexBufferedWriteKeyLimit(256)) //
                .writePath(writePath -> writePath.segmentSplitKeyThreshold(512)) //
                .segment(segment -> segment.maxKeys(128)) //
                .segment(segment -> segment.chunkKeyLimit(4)) //
                .segment(segment -> segment.deltaCacheFileLimit(1)) //
                .segment(segment -> segment.cachedSegmentLimit(16)) //
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024 * 128)) //
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(3)) //
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(false)) //
                .logging(logging -> logging.contextEnabled(false)) //
                .identity(identity -> identity.name("metrics_cache_only_runtime_test")) //
                .build();

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            for (int i = 0; i < 64; i++) {
                index.put(i, "v-" + i);
            }
            index.maintenance().flushAndWait();
            assertEquals("v-0", index.get(0));

            final SegmentIndexMetricsSnapshot before = index.runtimeMonitoring().snapshot().getMetrics();
            final SegmentIndexMetricsSnapshot after = index.runtimeMonitoring().snapshot().getMetrics();

            assertEquals(before.getRegistryCacheLoadCount(),
                    after.getRegistryCacheLoadCount());
            assertEquals(before.getRegistryCacheMissCount(),
                    after.getRegistryCacheMissCount());
        }
    }

    @Test
    void runtimeThresholdPatchTriggersFullSplitScanWithoutNewWrites() {
        final Directory directory = new MemDirectory();
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(keyDescriptor)) //
                .identity(identity -> identity.valueTypeDescriptor(valueDescriptor)) //
                .segment(segment -> segment.cacheKeyLimit(8)) //
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(32)) //
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(96)) //
                .writePath(writePath -> writePath.indexBufferedWriteKeyLimit(192)) //
                .segment(segment -> segment.maxKeys(128)) //
                .segment(segment -> segment.chunkKeyLimit(4)) //
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024 * 128)) //
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(3)) //
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(true)) //
                .logging(logging -> logging.contextEnabled(false)) //
                .identity(identity -> identity.name("metrics_runtime_split_policy_test")) //
                .build();

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            for (int i = 0; i < 48; i++) {
                index.put(i, "v-" + i);
            }
            index.maintenance().flushAndWait();
            awaitIdle(index);
            assertEquals(1, index.runtimeMonitoring().snapshot().getMetrics().getSegmentCount());

            final long revision = index.runtimeTuning()
                    .getCurrent().getRevision();
            final RuntimePatchResult patchResult = index.runtimeTuning()
                    .apply(new RuntimeConfigPatch(Map.of(
                            RuntimeSettingKey.SEGMENT_SPLIT_KEY_THRESHOLD,
                            Integer.valueOf(16)), false,
                            Long.valueOf(revision)));

            assertTrue(patchResult.isApplied());
            awaitCondition(() -> {
                final SegmentIndexMetricsSnapshot snapshot = index.runtimeMonitoring().snapshot().getMetrics();
                return snapshot.getSegmentCount() > 1
                        && snapshot.getSplitInFlightCount() == 0;
            }, 10_000L);

            for (int i = 0; i < 48; i++) {
                assertEquals("v-" + i, index.get(i));
            }
        }
    }

    @Test
    void autonomousSplitPolicyLoopSplitsOversizedStableRangeWithoutNewEvents() {
        final Directory directory = new MemDirectory();
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(keyDescriptor)) //
                .identity(identity -> identity.valueTypeDescriptor(valueDescriptor)) //
                .segment(segment -> segment.cacheKeyLimit(8)) //
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(32)) //
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(96)) //
                .writePath(writePath -> writePath.indexBufferedWriteKeyLimit(192)) //
                .segment(segment -> segment.maxKeys(16)) //
                .segment(segment -> segment.chunkKeyLimit(4)) //
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024 * 128)) //
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(3)) //
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(true)) //
                .logging(logging -> logging.contextEnabled(false)) //
                .identity(identity -> identity.name("metrics_autonomous_split_policy_test")) //
                .build();
        final String propertyName = "hestiastore.disableSplits";
        final String previousValue = System.getProperty(propertyName);
        System.setProperty(propertyName, Boolean.TRUE.toString());

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            for (int i = 0; i < 48; i++) {
                index.put(i, "v-" + i);
            }
            index.maintenance().flushAndWait();
            awaitIdle(index);
            assertEquals(1, index.runtimeMonitoring().snapshot().getMetrics().getSegmentCount());

            if (previousValue == null) {
                System.clearProperty(propertyName);
            } else {
                System.setProperty(propertyName, previousValue);
            }

            awaitCondition(() -> {
                final SegmentIndexMetricsSnapshot snapshot = index.runtimeMonitoring().snapshot().getMetrics();
                return snapshot.getSegmentCount() > 1
                        && snapshot.getSplitInFlightCount() == 0;
            }, 10_000L);

            for (int i = 0; i < 48; i++) {
                assertEquals("v-" + i, index.get(i));
            }
        } finally {
            if (previousValue == null) {
                System.clearProperty(propertyName);
            } else {
                System.setProperty(propertyName, previousValue);
            }
        }
    }

    @Test
    void flushAndWaitClearsBufferedWriteMetrics() {
        assertMaintenanceBoundaryClearsBufferedWriteMetrics(
                "metrics_flush_split_boundary_test",
                index -> index.maintenance().flushAndWait());
    }

    @Test
    void compactAndWaitClearsBufferedWriteMetrics() {
        assertMaintenanceBoundaryClearsBufferedWriteMetrics(
                "metrics_compact_split_boundary_test",
                index -> index.maintenance().compactAndWait());
    }

    private static void assertMaintenanceBoundaryClearsBufferedWriteMetrics(
            final String indexName,
            final Consumer<SegmentIndex<Integer, String>> maintenanceAction) {
        final Directory directory = new MemDirectory();
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(keyDescriptor)) //
                .identity(identity -> identity.valueTypeDescriptor(valueDescriptor)) //
                .segment(segment -> segment.cacheKeyLimit(8)) //
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(64)) //
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(96)) //
                .writePath(writePath -> writePath.indexBufferedWriteKeyLimit(192)) //
                .segment(segment -> segment.maxKeys(128)) //
                .segment(segment -> segment.chunkKeyLimit(4)) //
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024 * 128)) //
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(3)) //
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(false)) //
                .logging(logging -> logging.contextEnabled(false)) //
                .identity(identity -> identity.name(indexName)) //
                .build();

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            index.put(5, "buffered-5");
            index.put(18, "buffered-18");
            index.put(44, "buffered-44");
            index.put(49, "buffered-49");
            index.delete(18);

            final SegmentIndexMetricsSnapshot before = index.runtimeMonitoring().snapshot().getMetrics();
            assertTrue(before.getTotalBufferedWriteKeys() > 0L);

            maintenanceAction.accept(index);
            awaitIdle(index);

            final SegmentIndexMetricsSnapshot after = index.runtimeMonitoring().snapshot().getMetrics();
            assertEquals(0L, after.getTotalBufferedWriteKeys());
            assertEquals(0, after.getSplitInFlightCount());

            assertEquals("buffered-5", index.get(5));
            assertNull(index.get(18));
            assertEquals("buffered-44", index.get(44));
            assertEquals("buffered-49", index.get(49));
        }
    }

    private static void awaitIdle(final SegmentIndex<Integer, String> index) {
        awaitCondition(() -> {
            final SegmentIndexMetricsSnapshot snapshot = index.runtimeMonitoring().snapshot().getMetrics();
            return snapshot.getSplitInFlightCount() == 0;
        }, 10_000L);
    }

    private static void awaitCondition(final Supplier<Boolean> condition,
            final long timeoutMillis) {
        final long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (System.nanoTime() < deadline) {
            if (condition.get()) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20L));
            if (Thread.currentThread().isInterrupted()) {
                throw new AssertionError("Interrupted while waiting");
            }
        }
        assertTrue(condition.get(),
                "Condition not reached within " + timeoutMillis + " ms.");
    }

    private static void assertOperationCounts(
            final SegmentIndexMetricsSnapshot snapshot, final long putCount,
            final long getCount, final long deleteCount) {
        assertEquals(putCount, snapshot.getPutOperationCount());
        assertEquals(getCount, snapshot.getGetOperationCount());
        assertEquals(deleteCount, snapshot.getDeleteOperationCount());
    }

    private static void assertDirectWriteBufferMetrics(
            final SegmentIndexMetricsSnapshot snapshot) {
        assertTrue(snapshot.getTotalBufferedWriteKeys() >= 2L);
        assertEquals(0, snapshot.getSplitInFlightCount());
    }

    private static void assertRegistryAndBloomMetrics(
            final SegmentIndexMetricsSnapshot snapshot) {
        assertTrue(snapshot.getRegistryCacheHitCount() >= 0L);
        assertTrue(snapshot.getRegistryCacheMissCount() >= 0L);
        assertTrue(snapshot.getRegistryCacheLoadCount() >= 0L);
        assertTrue(snapshot.getRegistryCacheLimit() >= 1);
        assertTrue(snapshot.getBloomFilterRequestCount() >= 0L);
        assertTrue(snapshot.getBloomFilterRefusedCount() >= 0L);
        assertTrue(snapshot.getBloomFilterPositiveCount() >= 0L);
        assertTrue(snapshot.getBloomFilterFalsePositiveCount() >= 0L);
    }

    private static void assertWalDisabledSnapshot(
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

    private static void assertReadySnapshot(
            final SegmentIndexMetricsSnapshot snapshot) {
        assertTrue(snapshot.getSegmentRuntimeSnapshots()
                .size() <= snapshot.getSegmentCount());
        assertEquals(SegmentIndexState.READY, snapshot.getState());
    }
}
