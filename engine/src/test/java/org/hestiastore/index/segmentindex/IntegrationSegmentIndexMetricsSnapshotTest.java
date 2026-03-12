package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.function.Consumer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.RuntimePatchResult;
import org.hestiastore.index.control.model.RuntimeSettingKey;
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
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(keyDescriptor) //
                .withValueTypeDescriptor(valueDescriptor) //
                .withMaxNumberOfKeysInSegmentCache(8) //
                .withMaxNumberOfKeysInSegment(8) //
                .withMaxNumberOfKeysInSegmentChunk(4) //
                .withBloomFilterIndexSizeInBytes(1024 * 1024) //
                .withBloomFilterNumberOfHashFunctions(4) //
                .withContextLoggingEnabled(false) //
                .withName("metrics_test_index") //
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

            final SegmentIndexMetricsSnapshot snapshot = index
                    .metricsSnapshot();
            assertEquals(2L, snapshot.getPutOperationCount());
            assertEquals(4L, snapshot.getGetOperationCount());
            assertEquals(1L, snapshot.getDeleteOperationCount());
            assertTrue(snapshot.getTotalBufferedWriteKeys() >= 2L);
            assertTrue(snapshot.getPartitionCount() >= 1);
            assertTrue(snapshot.getActivePartitionCount() >= 1);
            assertTrue(snapshot.getPartitionBufferedKeyCount() >= 1);
            assertTrue(snapshot.getLocalThrottleCount() >= 0L);
            assertTrue(snapshot.getGlobalThrottleCount() >= 0L);
            assertTrue(snapshot.getDrainLatencyP95Micros() >= 0L);
            assertTrue(snapshot.getRegistryCacheHitCount() >= 0L);
            assertTrue(snapshot.getRegistryCacheMissCount() >= 0L);
            assertTrue(snapshot.getRegistryCacheLoadCount() >= 0L);
            assertTrue(snapshot.getRegistryCacheLimit() >= 1);
            assertTrue(snapshot.getBloomFilterRequestCount() >= 0L);
            assertTrue(snapshot.getBloomFilterRefusedCount() >= 0L);
            assertTrue(snapshot.getBloomFilterPositiveCount() >= 0L);
            assertTrue(snapshot.getBloomFilterFalsePositiveCount() >= 0L);
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
            assertTrue(snapshot.getSegmentRuntimeSnapshots()
                    .size() <= snapshot.getSegmentCount());
            assertEquals(SegmentIndexState.READY, snapshot.getState());
        }
    }

    @Test
    void metricsSnapshotExposesWalStatsWhenEnabled() {
        final Directory directory = new MemDirectory();
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(keyDescriptor) //
                .withValueTypeDescriptor(valueDescriptor) //
                .withMaxNumberOfKeysInSegment(64) //
                .withName("metrics_wal_enabled_test_index") //
                .withWal(Wal.builder().build()) //
                .build();

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            index.put(1, "a");
            index.put(2, "b");
            index.delete(1);
            index.flushAndWait();

            final SegmentIndexMetricsSnapshot snapshot = index
                    .metricsSnapshot();
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
    void drainScheduleCount_remains_monotonic_across_overlay_rotations() {
        final Directory directory = new MemDirectory();
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(keyDescriptor) //
                .withValueTypeDescriptor(valueDescriptor) //
                .withMaxNumberOfKeysInSegmentCache(5) //
                .withMaxNumberOfKeysInActivePartition(8) //
                .withMaxNumberOfImmutableRunsPerPartition(2) //
                .withMaxNumberOfKeysInPartitionBuffer(16) //
                .withMaxNumberOfKeysInIndexBuffer(64) //
                .withMaxNumberOfKeysInSegment(512) //
                .withMaxNumberOfKeysInSegmentChunk(4) //
                .withMaxNumberOfDeltaCacheFiles(1) //
                .withBloomFilterIndexSizeInBytes(1024 * 128) //
                .withBloomFilterNumberOfHashFunctions(3) //
                .withBackgroundMaintenanceAutoEnabled(true) //
                .withContextLoggingEnabled(false) //
                .withName("metrics_compaction_monotonicity_test") //
                .build();

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            for (int i = 0; i < 128; i++) {
                index.put(i, "base-" + i);
            }

            awaitIdle(index);
            final SegmentIndexMetricsSnapshot beforeRotation = index
                    .metricsSnapshot();
            assertTrue(beforeRotation.getDrainScheduleCount() > 0L);

            for (int i = 128; i < 256; i++) {
                index.put(i, "next-" + i);
            }
            awaitIdle(index);

            final SegmentIndexMetricsSnapshot afterRotation = index
                    .metricsSnapshot();
            assertTrue(
                    afterRotation.getDrainScheduleCount() >= beforeRotation
                            .getDrainScheduleCount());
            assertTrue(afterRotation.getDrainLatencyP95Micros() >= 0L);
        }
    }

    @Test
    void metricsSnapshot_doesNotLoadSegmentsOutsideRegistryCache() {
        final Directory directory = new MemDirectory();
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(keyDescriptor) //
                .withValueTypeDescriptor(valueDescriptor) //
                .withMaxNumberOfKeysInSegmentCache(5) //
                .withMaxNumberOfKeysInActivePartition(8) //
                .withMaxNumberOfImmutableRunsPerPartition(2) //
                .withMaxNumberOfKeysInPartitionBuffer(16) //
                .withMaxNumberOfKeysInIndexBuffer(64) //
                .withMaxNumberOfKeysInPartitionBeforeSplit(512) //
                .withMaxNumberOfKeysInSegment(20) //
                .withMaxNumberOfKeysInSegmentChunk(4) //
                .withMaxNumberOfDeltaCacheFiles(1) //
                .withMaxNumberOfSegmentsInCache(16) //
                .withBloomFilterIndexSizeInBytes(1024 * 128) //
                .withBloomFilterNumberOfHashFunctions(3) //
                .withBackgroundMaintenanceAutoEnabled(false) //
                .withContextLoggingEnabled(false) //
                .withName("metrics_cache_only_runtime_test") //
                .build();

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            for (int i = 0; i < 64; i++) {
                index.put(i, "v-" + i);
            }
            index.flushAndWait();
            assertEquals("v-0", index.get(0));

            final SegmentIndexMetricsSnapshot before = index.metricsSnapshot();
            final SegmentIndexMetricsSnapshot after = index.metricsSnapshot();

            assertEquals(before.getRegistryCacheLoadCount(),
                    after.getRegistryCacheLoadCount());
            assertEquals(before.getRegistryCacheMissCount(),
                    after.getRegistryCacheMissCount());
        }
    }

    @Test
    void runtimeThresholdPatchTriggersBackgroundSplitWithoutNewWrites() {
        final Directory directory = new MemDirectory();
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(keyDescriptor) //
                .withValueTypeDescriptor(valueDescriptor) //
                .withMaxNumberOfKeysInSegmentCache(8) //
                .withMaxNumberOfKeysInActivePartition(32) //
                .withMaxNumberOfImmutableRunsPerPartition(2) //
                .withMaxNumberOfKeysInPartitionBuffer(96) //
                .withMaxNumberOfKeysInIndexBuffer(192) //
                .withMaxNumberOfKeysInSegment(128) //
                .withMaxNumberOfKeysInSegmentChunk(4) //
                .withBloomFilterIndexSizeInBytes(1024 * 128) //
                .withBloomFilterNumberOfHashFunctions(3) //
                .withBackgroundMaintenanceAutoEnabled(true) //
                .withContextLoggingEnabled(false) //
                .withName("metrics_runtime_split_policy_test") //
                .build();

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            for (int i = 0; i < 48; i++) {
                index.put(i, "v-" + i);
            }
            index.flushAndWait();
            awaitIdle(index);
            assertEquals(1, index.metricsSnapshot().getSegmentCount());

            final long revision = index.controlPlane().configuration()
                    .getConfigurationActual().getRevision();
            final RuntimePatchResult patchResult = index.controlPlane()
                    .configuration()
                    .apply(new RuntimeConfigPatch(Map.of(
                            RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT,
                            Integer.valueOf(16)), false,
                            Long.valueOf(revision)));

            assertTrue(patchResult.isApplied());
            awaitCondition(() -> {
                final SegmentIndexMetricsSnapshot snapshot = index
                        .metricsSnapshot();
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
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(keyDescriptor) //
                .withValueTypeDescriptor(valueDescriptor) //
                .withMaxNumberOfKeysInSegmentCache(8) //
                .withMaxNumberOfKeysInActivePartition(32) //
                .withMaxNumberOfImmutableRunsPerPartition(2) //
                .withMaxNumberOfKeysInPartitionBuffer(96) //
                .withMaxNumberOfKeysInIndexBuffer(192) //
                .withMaxNumberOfKeysInSegment(16) //
                .withMaxNumberOfKeysInSegmentChunk(4) //
                .withBloomFilterIndexSizeInBytes(1024 * 128) //
                .withBloomFilterNumberOfHashFunctions(3) //
                .withBackgroundMaintenanceAutoEnabled(true) //
                .withContextLoggingEnabled(false) //
                .withName("metrics_autonomous_split_policy_test") //
                .build();
        final String propertyName = "hestiastore.disableSplits";
        final String previousValue = System.getProperty(propertyName);
        System.setProperty(propertyName, Boolean.TRUE.toString());

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            for (int i = 0; i < 48; i++) {
                index.put(i, "v-" + i);
            }
            index.flushAndWait();
            awaitIdle(index);
            assertEquals(1, index.metricsSnapshot().getSegmentCount());

            if (previousValue == null) {
                System.clearProperty(propertyName);
            } else {
                System.setProperty(propertyName, previousValue);
            }

            awaitCondition(() -> {
                final SegmentIndexMetricsSnapshot snapshot = index
                        .metricsSnapshot();
                return snapshot.getSegmentCount() > 1
                        && snapshot.getSplitInFlightCount() == 0
                        && snapshot.getDrainInFlightCount() == 0
                        && snapshot.getImmutableRunCount() == 0;
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
    void flushAndWaitClearsOverlayBacklogMetricsAfterScheduledSplit() {
        assertMaintenanceBoundaryClearsOverlayBacklogMetrics(
                "metrics_flush_split_boundary_test",
                SegmentIndex::flushAndWait);
    }

    @Test
    void compactAndWaitClearsOverlayBacklogMetricsAfterScheduledSplit() {
        assertMaintenanceBoundaryClearsOverlayBacklogMetrics(
                "metrics_compact_split_boundary_test",
                SegmentIndex::compactAndWait);
    }

    private static void assertMaintenanceBoundaryClearsOverlayBacklogMetrics(
            final String indexName,
            final Consumer<SegmentIndex<Integer, String>> maintenanceAction) {
        final Directory directory = new MemDirectory();
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(keyDescriptor) //
                .withValueTypeDescriptor(valueDescriptor) //
                .withMaxNumberOfKeysInSegmentCache(8) //
                .withMaxNumberOfKeysInActivePartition(32) //
                .withMaxNumberOfImmutableRunsPerPartition(2) //
                .withMaxNumberOfKeysInPartitionBuffer(96) //
                .withMaxNumberOfKeysInIndexBuffer(192) //
                .withMaxNumberOfKeysInSegment(128) //
                .withMaxNumberOfKeysInSegmentChunk(4) //
                .withBloomFilterIndexSizeInBytes(1024 * 128) //
                .withBloomFilterNumberOfHashFunctions(3) //
                .withBackgroundMaintenanceAutoEnabled(true) //
                .withContextLoggingEnabled(false) //
                .withName(indexName) //
                .build();

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            seedStableOverlaySplitScenario(index);

            final SegmentIndexMetricsSnapshot before = index.metricsSnapshot();
            assertTrue(before.getTotalBufferedWriteKeys() > 0L);
            assertTrue(before.getPartitionBufferedKeyCount() > 0);
            assertTrue(before.getActivePartitionCount() > 0);
            assertTrue(before.getSplitScheduleCount() > 0L
                    || before.getSplitInFlightCount() > 0
                    || before.getSegmentCount() > 1);

            maintenanceAction.accept(index);
            awaitIdle(index);

            final SegmentIndexMetricsSnapshot after = index.metricsSnapshot();
            assertEquals(0L, after.getTotalBufferedWriteKeys());
            assertEquals(0, after.getPartitionBufferedKeyCount());
            assertEquals(0, after.getImmutableRunCount());
            assertEquals(0, after.getDrainingPartitionCount());
            assertEquals(0, after.getDrainInFlightCount());
            assertEquals(0, after.getSplitInFlightCount());
            assertTrue(after.getSplitScheduleCount() >= before
                    .getSplitScheduleCount());
            assertTrue(after.getSegmentCount() > 1);
            assertTrue(after.getDrainLatencyP95Micros() >= 0L);

            assertEquals("overlay-5", index.get(5));
            assertNull(index.get(18));
            assertEquals("overlay-44", index.get(44));
            assertEquals("overlay-49", index.get(49));
        }
    }

    private static void seedStableOverlaySplitScenario(
            final SegmentIndex<Integer, String> index) {
        for (int i = 0; i < 48; i++) {
            index.put(i, "stable-" + i);
        }
        index.flushAndWait();
        awaitCondition(() -> index.metricsSnapshot().getSegmentCount() == 1
                && index.metricsSnapshot().getSplitInFlightCount() == 0
                && index.metricsSnapshot().getDrainInFlightCount() == 0,
                10_000L);

        index.put(5, "overlay-5");
        index.delete(18);
        index.put(44, "overlay-44");
        index.put(49, "overlay-49");

        final long revision = index.controlPlane().configuration()
                .getConfigurationActual().getRevision();
        final RuntimePatchResult patchResult = index.controlPlane()
                .configuration()
                .apply(new RuntimeConfigPatch(Map.of(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT,
                        Integer.valueOf(16)), false, Long.valueOf(revision)));
        assertTrue(patchResult.isApplied());

        awaitCondition(() -> {
            final SegmentIndexMetricsSnapshot snapshot = index
                    .metricsSnapshot();
            return snapshot.getSplitScheduleCount() > 0L
                    || snapshot.getSplitInFlightCount() > 0
                    || snapshot.getSegmentCount() > 1;
        }, 10_000L);
    }

    private static void awaitIdle(final SegmentIndex<Integer, String> index) {
        awaitCondition(() -> {
            final SegmentIndexMetricsSnapshot snapshot = index
                    .metricsSnapshot();
            return snapshot.getDrainInFlightCount() == 0
                    && snapshot.getImmutableRunCount() == 0
                    && snapshot.getDrainingPartitionCount() == 0
                    && snapshot.getSplitInFlightCount() == 0;
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
}
