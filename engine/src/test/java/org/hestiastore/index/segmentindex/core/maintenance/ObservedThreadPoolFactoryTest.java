package org.hestiastore.index.segmentindex.core.maintenance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.junit.jupiter.api.Test;

class ObservedThreadPoolFactoryTest {

    @Test
    void createAbortingPoolTracksRejectedTasks() throws InterruptedException {
        final ObservedThreadPool pool = new ObservedThreadPoolFactory(
                buildConf()).createAbortingPool(
                        IndexConfiguration::getNumberOfIndexMaintenanceThreads,
                        "indexMaintenanceThreads", "reject-test-");
        final CountDownLatch blocker = new CountDownLatch(1);
        try {
            pool.executor().execute(() -> await(blocker));
            for (int i = 0; i < 64; i++) {
                pool.executor().execute(() -> await(blocker));
            }

            assertThrows(RejectedExecutionException.class,
                    () -> pool.executor().execute(() -> {
                    }));
            assertEquals(1L, pool.snapshot().getRejectedTaskCount());
        } finally {
            blocker.countDown();
            pool.executor().shutdownNow();
        }
    }

    @Test
    void createCallerRunsPoolTracksCallerRuns() throws InterruptedException {
        final ObservedThreadPool pool = new ObservedThreadPoolFactory(
                buildConf()).createCallerRunsPool(
                        IndexConfiguration::getNumberOfSegmentMaintenanceThreads,
                        "segmentMaintenanceThreads", "caller-runs-test-");
        final CountDownLatch blocker = new CountDownLatch(1);
        try {
            pool.executor().execute(() -> await(blocker));
            for (int i = 0; i < 64; i++) {
                pool.executor().execute(() -> await(blocker));
            }

            pool.executor().execute(() -> {
            });

            assertEquals(1L, pool.snapshot().getCallerRunsCount());
        } finally {
            blocker.countDown();
            pool.executor().shutdownNow();
        }
    }

    @Test
    void daemonThreadFactoryCreatesNamedDaemonThreads() {
        final Thread thread = new ObservedThreadPoolFactory(buildConf())
                .daemonThreadFactory("observed-thread-").newThread(() -> {
                });

        assertTrue(thread.isDaemon());
        assertTrue(thread.getName().startsWith("observed-thread-"));
    }

    private static void await(final CountDownLatch blocker) {
        try {
            blocker.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName("observed-thread-pool-factory-test")
                .withContextLoggingEnabled(false)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInActivePartition(5)
                .withMaxNumberOfKeysInPartitionBuffer(6)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withMaxNumberOfKeysInSegment(100)
                .withMaxNumberOfSegmentsInCache(3)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSizeInBytes(1024)
                .withNumberOfIndexMaintenanceThreads(1)
                .withNumberOfSegmentMaintenanceThreads(1)
                .withNumberOfRegistryLifecycleThreads(1)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }
}
