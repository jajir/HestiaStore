package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
import org.hestiastore.index.segmentindex.core.session.IndexInternalConcurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.RuntimePatchResult;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentIndexImplConcurrencyTest {

    private IndexInternalConcurrent<Integer, String> index;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        index = new IndexInternalConcurrent<>(
                new MemDirectory(),
                new TypeDescriptorInteger(),
                new TypeDescriptorShortString(),
                conf, conf.resolveRuntimeConfiguration(),
                ExecutorRegistryFixture.from(conf));
    }

    @AfterEach
    void tearDown() {
        if (index != null && !index.wasClosed()) {
            index.close();
        }
    }

    @Test
    void parallelPutAndGetDoNotSerialize() throws Exception {
        final int threads = 4;
        final int keysPerThread = 25;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            final CountDownLatch start = new CountDownLatch(1);
            final CountDownLatch done = new CountDownLatch(threads);
            final AtomicReference<Throwable> failure = new AtomicReference<>();

            for (int t = 0; t < threads; t++) {
                final int base = t * keysPerThread;
                executor.execute(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < keysPerThread; i++) {
                            final int key = base + i;
                            final String value = "v" + key;
                            index.put(key, value);
                            final String stored = index.get(key);
                            if (!value.equals(stored)) {
                                throw new IllegalStateException(
                                        "Unexpected value for key " + key);
                            }
                        }
                    } catch (final Exception ex) {
                        failure.compareAndSet(null, ex);
                    } finally {
                        done.countDown();
                    }
                });
            }

            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS),
                    "Parallel writes did not finish");
            if (failure.get() != null) {
                throw new AssertionError("Parallel operations failed",
                        failure.get());
            }

            // Ensure async maintenance/flush completes before final full scan.
            index.flushAndWait();

            for (int key = 0; key < threads * keysPerThread; key++) {
                assertEquals("v" + key, index.get(key));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void writesAndReadsSucceedBeforeAndAfterSplitRemapsRoutes() {
        recreateIndex(buildAutonomousSplitConf());
        seedStableRange(48);
        awaitCondition(() -> index.metricsSnapshot().getSegmentCount() == 1
                && index.metricsSnapshot().getSplitInFlightCount() == 0
                && index.metricsSnapshot().getDrainInFlightCount() == 0,
                10_000L);
        assertEquals("stable-30", index.get(30));

        setSplitThreshold(32);

        boolean splitObserved = false;
        for (int i = 0; i < 256; i++) {
            index.put(49, "trigger-49-" + i);
            final SegmentIndexMetricsSnapshot snapshot = index
                    .metricsSnapshot();
            if (snapshot.getSplitScheduleCount() > 0
                    || snapshot.getSplitInFlightCount() > 0
                    || snapshot.getSegmentCount() > 1) {
                splitObserved = true;
                break;
            }
        }

        assertTrue(splitObserved,
                "Expected split scheduling while writes were in progress.");
        setSplitThreshold(10_000_000);
        awaitCondition(() -> index.metricsSnapshot().getSegmentCount() > 1,
                30_000L);

        final String finalValue = "final-5";
        index.put(5, finalValue);
        assertEquals(finalValue, index.get(5));
        index.delete(18);
        assertNull(index.get(18));
        index.put(49, "final-49");
        assertEquals("final-49", index.get(49));
        awaitCondition(() -> finalValue.equals(index.get(5))
                && index.get(18) == null
                && "final-49".equals(index.get(49)), 10_000L);
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withName("segment-index-concurrency-test")//
                .withContextLoggingEnabled(false)//
                .withMaxNumberOfKeysInSegmentCache(100)//
                .withMaxNumberOfKeysInActivePartition(200)//
                .withMaxNumberOfKeysInPartitionBuffer(220)//
                // Keep this test focused on concurrent put/get behavior and
                // avoid background maintenance races while workers are running.
                .withBackgroundMaintenanceAutoEnabled(false)//
                .withMaxNumberOfKeysInSegmentChunk(10)//
                .withMaxNumberOfKeysInSegment(1000)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }

    private IndexConfiguration<Integer, String> buildAutonomousSplitConf() {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withName("segment-index-concurrency-split-remap-test")//
                .withContextLoggingEnabled(false)//
                .withMaxNumberOfKeysInSegmentCache(8)//
                .withMaxNumberOfKeysInActivePartition(32)//
                .withMaxNumberOfImmutableRunsPerPartition(2)//
                .withMaxNumberOfKeysInPartitionBuffer(96)//
                .withMaxNumberOfKeysInIndexBuffer(192)//
                .withMaxNumberOfKeysInPartitionBeforeSplit(512)//
                .withBackgroundMaintenanceAutoEnabled(true)//
                .withMaxNumberOfKeysInSegmentChunk(4)//
                .withMaxNumberOfKeysInSegment(128)//
                .withMaxNumberOfSegmentsInCache(8)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withNumberOfIndexMaintenanceThreads(2)//
                .withNumberOfRegistryLifecycleThreads(2)//
                .withIndexBusyTimeoutMillis(30_000)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }

    private void recreateIndex(final IndexConfiguration<Integer, String> conf) {
        if (index != null && !index.wasClosed()) {
            index.close();
        }
        index = new IndexInternalConcurrent<>(
                new MemDirectory(),
                new TypeDescriptorInteger(),
                new TypeDescriptorShortString(),
                conf, conf.resolveRuntimeConfiguration(),
                ExecutorRegistryFixture.from(conf));
    }

    private void seedStableRange(final int count) {
        for (int key = 0; key < count; key++) {
            index.put(key, "stable-" + key);
        }
        index.flushAndWait();
    }

    private void setSplitThreshold(final int threshold) {
        final long revision = index.controlPlane().configuration()
                .getConfigurationActual().getRevision();
        final RuntimePatchResult patchResult = index.controlPlane()
                .configuration()
                .apply(new RuntimeConfigPatch(Map.of(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT,
                        Integer.valueOf(threshold)), false,
                        Long.valueOf(revision)));
        assertTrue(patchResult.isApplied());
    }

    private static void awaitCondition(final Supplier<Boolean> condition,
            final long timeoutMillis) {
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
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
