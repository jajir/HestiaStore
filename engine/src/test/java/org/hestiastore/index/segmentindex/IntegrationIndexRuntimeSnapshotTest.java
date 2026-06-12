package org.hestiastore.index.segmentindex;

import org.hestiastore.index.segmentindex.runtimemonitoring.model.IndexRuntimeSnapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.user.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningPatch;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningResult;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class IntegrationIndexRuntimeSnapshotTest {

    @Test
    void runtimeSnapshotCountsPointOperations() {
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

            final IndexRuntimeSnapshot snapshot = index.runtimeMonitoring().snapshot();
            assertOperationCounts(snapshot, 2L, 4L, 1L);
            assertDirectWriteBufferMetrics(snapshot);
            assertRegistryAndBloomMetrics(snapshot);
            assertWalDisabledSnapshot(snapshot);
            assertReadySnapshot(snapshot);
        }
    }

    @Test
    void runtimeSnapshotExposesExecutorRuntimeMetrics() {
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
            final IndexRuntimeSnapshot snapshot = index.runtimeMonitoring().snapshot();

            assertTrue(snapshot.maintenance().indexExecutor()
                    .queueCapacity() > 0);
            assertTrue(snapshot.split().executor().queueCapacity() > 0);
            assertTrue(snapshot.maintenance().stableSegmentExecutor()
                    .queueCapacity() > 0);
            assertTrue(snapshot.maintenance().indexExecutor().queueSize() >= 0);
            assertTrue(snapshot.split().executor().queueSize() >= 0);
            assertTrue(snapshot.maintenance().stableSegmentExecutor()
                    .queueSize() >= 0);
            assertTrue(snapshot.maintenance().indexExecutor()
                    .activeThreadCount() >= 0);
            assertTrue(snapshot.split().executor().activeThreadCount() >= 0);
            assertTrue(
                    snapshot.maintenance().stableSegmentExecutor()
                            .activeThreadCount() >= 0);
            assertTrue(snapshot.maintenance().indexExecutor()
                    .completedTaskCount() >= 0L);
            assertTrue(snapshot.split().executor().completedTaskCount() >= 0L);
            assertTrue(snapshot
                    .maintenance().stableSegmentExecutor()
                    .completedTaskCount() >= 0L);
            assertTrue(snapshot.maintenance().indexExecutor()
                    .rejectedTaskCount() >= 0L);
            assertTrue(snapshot.split().executor().rejectedTaskCount() >= 0L);
            assertTrue(
                    snapshot.maintenance().stableSegmentExecutor()
                            .callerRunsCount() >= 0L);
            assertTrue(snapshot.split().taskStartDelayP95Micros() >= 0L);
            assertTrue(snapshot.split().taskRunLatencyP95Micros() >= 0L);
            assertTrue(snapshot.maintenance().flushAcceptedToReadyP95Micros() >= 0L);
            assertTrue(snapshot.maintenance().compactAcceptedToReadyP95Micros() >= 0L);
            assertTrue(snapshot.maintenance().flushBusyRetryCount() >= 0L);
            assertTrue(snapshot.maintenance().compactBusyRetryCount() >= 0L);
        }
    }

    @Test
    void runtimeSnapshotExposesWalMonitoringWhenEnabled() {
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

            final IndexRuntimeSnapshot snapshot = index.runtimeMonitoring().snapshot();
            assertTrue(snapshot.wal().enabled());
            assertTrue(snapshot.wal().appendCount() >= 3L);
            assertTrue(snapshot.wal().appendBytes() > 0L);
            assertTrue(snapshot.wal().syncCount() >= 1L);
            assertEquals(0L, snapshot.wal().syncFailureCount());
            assertEquals(0L, snapshot.wal().corruptionCount());
            assertEquals(0L, snapshot.wal().truncationCount());
            assertTrue(snapshot.wal().segmentCount() >= 1);
            assertTrue(snapshot.wal().durableLsn() >= 3L);
            assertTrue(snapshot.wal().checkpointLsn() >= 3L);
            assertTrue(snapshot.wal().appliedLsn() >= 3L);
            assertTrue(snapshot.wal().checkpointLagLsn() >= 0L);
            assertTrue(snapshot.wal().syncTotalNanos() >= 0L);
            assertTrue(snapshot.wal().syncMaxNanos() >= 0L);
            assertTrue(snapshot.wal().syncBatchBytesTotal() > 0L);
            assertTrue(snapshot.wal().syncBatchBytesMax() > 0L);
            assertTrue(snapshot.wal().syncBatchBytesTotal() >= snapshot
                    .wal().syncBatchBytesMax());
            assertTrue(snapshot.wal().syncAverageNanos() >= 0L);
            assertTrue(snapshot.wal().syncAverageBatchBytes() > 0L);
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
            final IndexRuntimeSnapshot beforeRotation = index.runtimeMonitoring().snapshot();
            assertEquals(0, beforeRotation.split().inFlightCount());

            for (int i = 128; i < 256; i++) {
                index.put(i, "next-" + i);
            }
            awaitIdle(index);

            final IndexRuntimeSnapshot afterRotation = index.runtimeMonitoring().snapshot();
            assertEquals(0, afterRotation.split().inFlightCount());
        }
    }

    @Test
    void runtimeSnapshotDoesNotLoadSegmentsOutsideRegistryCache() {
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

            final IndexRuntimeSnapshot before = index.runtimeMonitoring().snapshot();
            final IndexRuntimeSnapshot after = index.runtimeMonitoring().snapshot();

            assertEquals(before.registryCache().loadCount(),
                    after.registryCache().loadCount());
            assertEquals(before.registryCache().missCount(),
                    after.registryCache().missCount());
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
            assertEquals(1, index.runtimeMonitoring().snapshot().segments().count());

            final long revision = index.runtimeTuning()
                    .current().revision();
            final RuntimeTuningResult patchResult = index.runtimeTuning()
                    .apply(RuntimeTuningPatch.builder()
                            .expectedRevision(revision)
                            .writePath(writePath -> writePath
                                    .segmentSplitKeyThreshold(16))
                            .build());

            assertTrue(patchResult.applied());
            awaitCondition(() -> {
                final IndexRuntimeSnapshot snapshot = index.runtimeMonitoring().snapshot();
                return snapshot.segments().count() > 1
                        && snapshot.split().inFlightCount() == 0;
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
            assertEquals(1, index.runtimeMonitoring().snapshot().segments().count());

            if (previousValue == null) {
                System.clearProperty(propertyName);
            } else {
                System.setProperty(propertyName, previousValue);
            }

            awaitCondition(() -> {
                final IndexRuntimeSnapshot snapshot = index.runtimeMonitoring().snapshot();
                return snapshot.segments().count() > 1
                        && snapshot.split().inFlightCount() == 0;
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

            final IndexRuntimeSnapshot before = index.runtimeMonitoring().snapshot();
            assertTrue(before.writePath().totalBufferedWriteKeys() > 0L);

            maintenanceAction.accept(index);
            awaitIdle(index);

            final IndexRuntimeSnapshot after = index.runtimeMonitoring().snapshot();
            assertEquals(0L, after.writePath().totalBufferedWriteKeys());
            assertEquals(0, after.split().inFlightCount());

            assertEquals("buffered-5", index.get(5));
            assertNull(index.get(18));
            assertEquals("buffered-44", index.get(44));
            assertEquals("buffered-49", index.get(49));
        }
    }

    private static void awaitIdle(final SegmentIndex<Integer, String> index) {
        awaitCondition(() -> {
            final IndexRuntimeSnapshot snapshot = index.runtimeMonitoring().snapshot();
            return snapshot.split().inFlightCount() == 0;
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
            final IndexRuntimeSnapshot snapshot, final long putCount,
            final long getCount, final long deleteCount) {
        assertEquals(putCount, snapshot.operations().putOperationCount());
        assertEquals(getCount, snapshot.operations().readOperationCount());
        assertEquals(deleteCount, snapshot.operations().deleteOperationCount());
    }

    private static void assertDirectWriteBufferMetrics(
            final IndexRuntimeSnapshot snapshot) {
        assertTrue(snapshot.writePath().totalBufferedWriteKeys() >= 2L);
        assertEquals(0, snapshot.split().inFlightCount());
    }

    private static void assertRegistryAndBloomMetrics(
            final IndexRuntimeSnapshot snapshot) {
        assertTrue(snapshot.registryCache().hitCount() >= 0L);
        assertTrue(snapshot.registryCache().missCount() >= 0L);
        assertTrue(snapshot.registryCache().loadCount() >= 0L);
        assertTrue(snapshot.registryCache().limit() >= 1);
        assertTrue(snapshot.bloomFilter().requestCount() >= 0L);
        assertTrue(snapshot.bloomFilter().refusedCount() >= 0L);
        assertTrue(snapshot.bloomFilter().positiveCount() >= 0L);
        assertTrue(snapshot.bloomFilter().falsePositiveCount() >= 0L);
    }

    private static void assertWalDisabledSnapshot(
            final IndexRuntimeSnapshot snapshot) {
        assertEquals(false, snapshot.wal().enabled());
        assertEquals(0L, snapshot.wal().appendCount());
        assertEquals(0L, snapshot.wal().appendBytes());
        assertEquals(0L, snapshot.wal().syncCount());
        assertEquals(0L, snapshot.wal().syncFailureCount());
        assertEquals(0L, snapshot.wal().corruptionCount());
        assertEquals(0L, snapshot.wal().truncationCount());
        assertEquals(0L, snapshot.wal().retainedBytes());
        assertEquals(0, snapshot.wal().segmentCount());
        assertEquals(0L, snapshot.wal().durableLsn());
        assertEquals(0L, snapshot.wal().checkpointLsn());
        assertEquals(0L, snapshot.wal().pendingSyncBytes());
        assertEquals(0L, snapshot.wal().appliedLsn());
        assertEquals(0L, snapshot.wal().checkpointLagLsn());
        assertEquals(0L, snapshot.wal().syncTotalNanos());
        assertEquals(0L, snapshot.wal().syncMaxNanos());
        assertEquals(0L, snapshot.wal().syncBatchBytesTotal());
        assertEquals(0L, snapshot.wal().syncBatchBytesMax());
        assertEquals(0L, snapshot.wal().syncAverageNanos());
        assertEquals(0L, snapshot.wal().syncAverageBatchBytes());
    }

    private static void assertReadySnapshot(
            final IndexRuntimeSnapshot snapshot) {
        assertTrue(snapshot.segments().runtimeMetrics()
                .size() <= snapshot.segments().count());
        assertEquals(SegmentIndexState.READY, snapshot.state());
    }
}
