package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Supplier;

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
            assertTrue(snapshot.getRegistryCacheHitCount() >= 1L);
            assertTrue(snapshot.getRegistryCacheMissCount() >= 1L);
            assertTrue(snapshot.getRegistryCacheLoadCount() >= 1L);
            assertTrue(snapshot.getRegistryCacheSize() >= 1);
            assertTrue(snapshot.getRegistryCacheLimit() >= 1);
            assertTrue(snapshot.getBloomFilterRequestCount() >= 0L);
            assertTrue(snapshot.getBloomFilterRefusedCount() >= 0L);
            assertTrue(snapshot.getBloomFilterPositiveCount() >= 0L);
            assertTrue(snapshot.getBloomFilterFalsePositiveCount() >= 0L);
            assertEquals(SegmentIndexState.READY, snapshot.getState());
        }
    }

    @Test
    void compactRequestCount_remains_monotonic_across_split() {
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
                .withMaxNumberOfKeysInSegmentWriteCache(64) //
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(128) //
                .withMaxNumberOfKeysInSegment(20) //
                .withMaxNumberOfKeysInSegmentChunk(4) //
                .withMaxNumberOfDeltaCacheFiles(1) //
                .withBloomFilterIndexSizeInBytes(1024 * 128) //
                .withBloomFilterNumberOfHashFunctions(3) //
                .withSegmentMaintenanceAutoEnabled(true) //
                .withContextLoggingEnabled(false) //
                .withName("metrics_compaction_monotonicity_test") //
                .build();

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            for (int i = 0; i < 10; i++) {
                index.put(i, "base-" + i);
            }
            for (int i = 0; i < 120; i++) {
                final int key = i % 10;
                index.put(key, "v-" + i);
            }

            awaitIdle(index);
            final SegmentIndexMetricsSnapshot beforeSplit = index
                    .metricsSnapshot();
            assertTrue(beforeSplit.getCompactRequestCount() > 0L);

            for (int i = 10; i < 30; i++) {
                index.put(i, "new-" + i);
            }

            awaitCondition(
                    () -> index.metricsSnapshot().getSegmentCount() > 1,
                    10_000L);
            awaitIdle(index);

            final SegmentIndexMetricsSnapshot afterSplit = index
                    .metricsSnapshot();
            assertTrue(
                    afterSplit.getCompactRequestCount() >= beforeSplit
                            .getCompactRequestCount());
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
                .withMaxNumberOfKeysInSegmentWriteCache(64) //
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(128) //
                .withMaxNumberOfKeysInSegment(20) //
                .withMaxNumberOfKeysInSegmentChunk(4) //
                .withMaxNumberOfDeltaCacheFiles(1) //
                .withMaxNumberOfSegmentsInCache(3) //
                .withBloomFilterIndexSizeInBytes(1024 * 128) //
                .withBloomFilterNumberOfHashFunctions(3) //
                .withSegmentMaintenanceAutoEnabled(true) //
                .withContextLoggingEnabled(false) //
                .withName("metrics_cache_only_runtime_test") //
                .build();

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            for (int i = 0; i < 300; i++) {
                index.put(i, "v-" + i);
            }

            awaitCondition(
                    () -> {
                        final SegmentIndexMetricsSnapshot snapshot = index
                                .metricsSnapshot();
                        return snapshot.getSegmentCount() > snapshot
                                .getRegistryCacheLimit();
                    },
                    10_000L);
            awaitIdle(index);

            final SegmentIndexMetricsSnapshot before = index.metricsSnapshot();
            final SegmentIndexMetricsSnapshot after = index.metricsSnapshot();

            assertTrue(
                    before.getSegmentCount() > before.getRegistryCacheLimit());
            assertEquals(before.getRegistryCacheLoadCount(),
                    after.getRegistryCacheLoadCount());
            assertEquals(before.getRegistryCacheMissCount(),
                    after.getRegistryCacheMissCount());
        }
    }

    private static void awaitIdle(final SegmentIndex<Integer, String> index) {
        awaitCondition(() -> {
            final SegmentIndexMetricsSnapshot snapshot = index
                    .metricsSnapshot();
            return snapshot.getSplitInFlightCount() == 0
                    && snapshot.getMaintenanceQueueSize() == 0
                    && snapshot.getSplitQueueSize() == 0;
        }, 10_000L);
    }

    private static void awaitCondition(final Supplier<Boolean> condition,
            final long timeoutMillis) {
        final long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (System.nanoTime() < deadline) {
            if (condition.get()) {
                return;
            }
            try {
                Thread.sleep(20L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting", e);
            }
        }
        assertTrue(condition.get(),
                "Condition not reached within " + timeoutMillis + " ms.");
    }
}
