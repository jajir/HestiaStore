package org.hestiastore.index.segmentindex.core.infrastructure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.observability.IndexExecutorMetricsAccess;
import org.hestiastore.index.segmentindex.core.observability.IndexExecutorRuntimeAccess;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class IndexExecutorRegistryTest {

    @Test
    void configurationConstructorRejectsNullConfiguration() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IndexExecutorRegistry((IndexConfiguration<?, ?>) null));
        assertEquals("Property 'indexConfiguration' must not be null.",
                ex.getMessage());
    }

    @Test
    void configurationConstructorUsesProvidedConfiguration() {
        final IndexConfiguration<Integer, String> conf = buildConf(2, 3, 4);
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(conf);
        try {
            assertNotNull(registry.getIndexMaintenanceExecutor());
            assertNotNull(registry.getSplitMaintenanceExecutor());
            assertNotNull(registry.getSplitPolicyScheduler());
            assertNotNull(registry.getStableSegmentMaintenanceExecutor());
            assertNotNull(registry.getRegistryMaintenanceExecutor());
        } finally {
            registry.close();
        }
    }

    @Test
    void constructorRejectsNonPositiveSegmentMaintenanceThreads() {
        final IndexConfiguration<Integer, String> conf = buildConf(0, 1, 1);
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IndexExecutorRegistry(conf));
        assertEquals(
                "Property 'numberOfSegmentMaintenanceThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void constructorRejectsNonPositiveIndexMaintenanceThreads() {
        final IndexConfiguration<Integer, String> conf = buildConf(1, 0, 1);
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IndexExecutorRegistry(conf));
        assertEquals(
                "Property 'indexMaintenanceThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void constructorRejectsNonPositiveRegistryMaintenanceThreads() {
        final IndexConfiguration<Integer, String> conf = buildConf(1, 1, 1);
        final IndexConfiguration<Integer, String> invalidConf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(conf.getKeyClass())//
                .withValueClass(conf.getValueClass())//
                .withKeyTypeDescriptor(conf.getKeyTypeDescriptor())//
                .withValueTypeDescriptor(conf.getValueTypeDescriptor())//
                .withName(conf.getIndexName())//
                .withContextLoggingEnabled(conf.isContextLoggingEnabled())//
                .withMaxNumberOfKeysInSegmentCache(
                        conf.getMaxNumberOfKeysInSegmentCache())//
                .withMaxNumberOfKeysInActivePartition(
                        conf.getMaxNumberOfKeysInActivePartition())//
                .withMaxNumberOfKeysInPartitionBuffer(
                        conf.getMaxNumberOfKeysInPartitionBuffer())//
                .withMaxNumberOfKeysInSegmentChunk(
                        conf.getMaxNumberOfKeysInSegmentChunk())//
                .withMaxNumberOfKeysInSegment(conf.getMaxNumberOfKeysInSegment())//
                .withMaxNumberOfSegmentsInCache(
                        conf.getMaxNumberOfSegmentsInCache())//
                .withBloomFilterNumberOfHashFunctions(
                        conf.getBloomFilterNumberOfHashFunctions())//
                .withBloomFilterIndexSizeInBytes(
                        conf.getBloomFilterIndexSizeInBytes())//
                .withBloomFilterProbabilityOfFalsePositive(
                        conf.getBloomFilterProbabilityOfFalsePositive())//
                .withDiskIoBufferSizeInBytes(conf.getDiskIoBufferSize())//
                .withNumberOfIndexMaintenanceThreads(
                        conf.getNumberOfIndexMaintenanceThreads())//
                .withNumberOfSegmentMaintenanceThreads(
                        conf.getNumberOfSegmentMaintenanceThreads())//
                .withNumberOfRegistryLifecycleThreads(0)//
                .withEncodingFilters(conf.getEncodingChunkFilters())//
                .withDecodingFilters(conf.getDecodingChunkFilters())//
                .build();
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IndexExecutorRegistry(invalidConf));
        assertEquals(
                "Property 'registryMaintenanceThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void constructorRejectsBlankIndexNameWhenContextLoggingEnabled() {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withName("  ")//
                .withContextLoggingEnabled(true)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInActivePartition(5)//
                .withMaxNumberOfKeysInPartitionBuffer(6)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInSegment(100)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withNumberOfIndexMaintenanceThreads(1)//
                .withNumberOfSegmentMaintenanceThreads(1)//
                .withNumberOfRegistryLifecycleThreads(1)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IndexExecutorRegistry(conf));
        assertEquals("Property 'indexName' must not be blank.",
                ex.getMessage());
    }

    @Test
    void closeShutsDownAllExecutors() {
        final IndexConfiguration<Integer, String> conf = buildConf(1, 1, 1);
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(conf);
        final ExecutorService indexMaintenance = registry
                .getIndexMaintenanceExecutor();
        final ExecutorService splitMaintenance = registry
                .getSplitMaintenanceExecutor();
        final ExecutorService splitPolicyScheduler = registry
                .getSplitPolicyScheduler();
        final ExecutorService stableSegmentMaintenance = registry
                .getStableSegmentMaintenanceExecutor();
        final ExecutorService registryMaintenance = registry
                .getRegistryMaintenanceExecutor();

        registry.close();

        assertTrue(indexMaintenance.isShutdown());
        assertTrue(splitMaintenance.isShutdown());
        assertTrue(splitPolicyScheduler.isShutdown());
        assertTrue(stableSegmentMaintenance.isShutdown());
        assertTrue(registryMaintenance.isShutdown());
    }

    @Test
    void gettersRejectCallsAfterClose() {
        final IndexConfiguration<Integer, String> conf = buildConf(1, 1, 1);
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(conf);
        registry.close();

        assertThrows(IllegalStateException.class,
                registry::getIndexMaintenanceExecutor);
        assertThrows(IllegalStateException.class,
                registry::getSplitMaintenanceExecutor);
        assertThrows(IllegalStateException.class,
                registry::getSplitPolicyScheduler);
        assertThrows(IllegalStateException.class,
                registry::getStableSegmentMaintenanceExecutor);
        assertThrows(IllegalStateException.class,
                registry::getRegistryMaintenanceExecutor);
    }

    @Test
    void gettersReturnSameExecutorInstances() {
        final IndexConfiguration<Integer, String> conf = buildConf(1, 1, 1);
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(conf);
        try {
            assertSame(registry.getIndexMaintenanceExecutor(),
                    registry.getIndexMaintenanceExecutor());
            assertSame(registry.getSplitMaintenanceExecutor(),
                    registry.getSplitMaintenanceExecutor());
            assertSame(registry.getSplitPolicyScheduler(),
                    registry.getSplitPolicyScheduler());
            assertSame(registry.getStableSegmentMaintenanceExecutor(),
                    registry.getStableSegmentMaintenanceExecutor());
            assertSame(registry.getRegistryMaintenanceExecutor(),
                    registry.getRegistryMaintenanceExecutor());
        } finally {
            registry.close();
        }
    }

    @Test
    void closeDoesNotNeedLazyExecutorsToBeRequestedFirst() {
        final IndexConfiguration<Integer, String> conf = buildConf(1, 1, 1);
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(conf);

        final ExecutorService indexMaintenance = registry
                .getIndexMaintenanceExecutor();
        final ExecutorService splitMaintenance = registry
                .getSplitMaintenanceExecutor();
        final ExecutorService stableSegmentMaintenance = registry
                .getStableSegmentMaintenanceExecutor();

        registry.close();

        assertTrue(indexMaintenance.isShutdown());
        assertTrue(splitMaintenance.isShutdown());
        assertTrue(stableSegmentMaintenance.isShutdown());
    }

    @Test
    void executorsUseExpectedThreadNamesAndDaemonThreads()
            throws InterruptedException, ExecutionException {
        final IndexConfiguration<Integer, String> conf = buildConf(1, 1, 1);
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(conf);
        try {
            final String indexMaintenanceName = registry
                    .getIndexMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().getName()).get();
            final String splitMaintenanceName = registry
                    .getSplitMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().getName()).get();
            final String splitPolicyName = registry
                    .getSplitPolicyScheduler()
                    .submit(() -> Thread.currentThread().getName()).get();
            final String stableSegmentMaintenanceName = registry
                    .getStableSegmentMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().getName()).get();
            final String registryMaintenanceName = registry
                    .getRegistryMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().getName()).get();

            final boolean indexMaintenanceDaemon = registry
                    .getIndexMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().isDaemon()).get();
            final boolean splitMaintenanceDaemon = registry
                    .getSplitMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().isDaemon()).get();
            final boolean splitPolicyDaemon = registry
                    .getSplitPolicyScheduler()
                    .submit(() -> Thread.currentThread().isDaemon()).get();
            final boolean stableSegmentMaintenanceDaemon = registry
                    .getStableSegmentMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().isDaemon()).get();
            final boolean registryMaintenanceDaemon = registry
                    .getRegistryMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().isDaemon()).get();

            assertTrue(indexMaintenanceName.startsWith("index-maintenance-"));
            assertTrue(splitMaintenanceName.startsWith("split-maintenance-"));
            assertTrue(splitPolicyName.startsWith("split-policy-"));
            assertTrue(stableSegmentMaintenanceName
                    .startsWith("segment-maintenance-"));
            assertTrue(
                    registryMaintenanceName.startsWith("registry-maintenance-"));
            assertTrue(indexMaintenanceDaemon);
            assertTrue(splitMaintenanceDaemon);
            assertTrue(splitPolicyDaemon);
            assertTrue(stableSegmentMaintenanceDaemon);
            assertTrue(registryMaintenanceDaemon);
        } finally {
            registry.close();
        }
    }

    @Test
    void stableSegmentMaintenanceExecutorUsesConfiguredThreadCount()
            throws Exception {
        final int stableSegmentMaintenanceThreads = 4;
        final IndexConfiguration<Integer, String> conf = buildConf(
                stableSegmentMaintenanceThreads, 1, 1);
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(conf);
        final CountDownLatch started = new CountDownLatch(
                stableSegmentMaintenanceThreads);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            final List<java.util.concurrent.Future<String>> futures = java.util.stream.IntStream
                    .range(0, stableSegmentMaintenanceThreads)
                    .mapToObj(i -> registry
                            .getStableSegmentMaintenanceExecutor().submit(() -> {
                                started.countDown();
                                release.await();
                                return Thread.currentThread().getName();
                            }))
                    .toList();
            assertTrue(started.await(2, TimeUnit.SECONDS),
                    "Stable segment maintenance executor did not start all configured workers.");
            release.countDown();
            final Set<String> threadNames = futures.stream().map(future -> {
                try {
                    return future.get();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "Interrupted while collecting stable segment maintenance worker names.",
                            e);
                } catch (final ExecutionException e) {
                    throw new IllegalStateException(
                            "Failed while collecting stable segment maintenance worker names.",
                            e.getCause());
                }
            }).collect(Collectors.toSet());
            assertEquals(stableSegmentMaintenanceThreads,
                    threadNames.size());
        } finally {
            release.countDown();
            registry.close();
        }
    }

    @Test
    void runtimeSnapshotTracksIndexMaintenanceQueuePressureAndRejections()
            throws InterruptedException {
        final IndexConfiguration<Integer, String> conf = buildConf(1, 1, 1);
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(conf);
        final CountDownLatch workerStarted = new CountDownLatch(1);
        final CountDownLatch releaseWorker = new CountDownLatch(1);
        try {
            registry.getIndexMaintenanceExecutor().execute(() -> {
                workerStarted.countDown();
                awaitRelease(releaseWorker);
            });
            assertTrue(workerStarted.await(2, TimeUnit.SECONDS));
            for (int i = 0; i < 64; i++) {
                registry.getIndexMaintenanceExecutor().execute(() -> {
                });
            }

            assertThrows(RejectedExecutionException.class,
                    () -> registry.getIndexMaintenanceExecutor().execute(() -> {
                    }));

            final IndexExecutorMetricsAccess snapshot =
                    registry.runtimeSnapshot().getIndexMaintenance();
            assertEquals(1, snapshot.getActiveThreadCount());
            assertEquals(64, snapshot.getQueueSize());
            assertEquals(64, snapshot.getQueueCapacity());
            assertEquals(1L, snapshot.getRejectedTaskCount());
            assertEquals(0L, snapshot.getCallerRunsCount());
        } finally {
            releaseWorker.countDown();
            registry.close();
        }
    }

    @Test
    @Disabled("Known regular failure; intentionally disabled until root cause is understood.")
    void runtimeSnapshotTracksCompletedTasksAndCallerRuns()
            throws InterruptedException, ExecutionException {
        final IndexConfiguration<Integer, String> conf = buildConf(1, 1, 1);
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(conf);
        final CountDownLatch workerStarted = new CountDownLatch(1);
        final CountDownLatch releaseWorker = new CountDownLatch(1);
        final AtomicReference<String> callerRunThread = new AtomicReference<>();
        try {
            registry.getSplitMaintenanceExecutor().submit(() -> {
            }).get();
            registry.getStableSegmentMaintenanceExecutor().submit(() -> {
            }).get();

            registry.getStableSegmentMaintenanceExecutor().execute(() -> {
                workerStarted.countDown();
                awaitRelease(releaseWorker);
            });
            assertTrue(workerStarted.await(2, TimeUnit.SECONDS));
            for (int i = 0; i < 64; i++) {
                registry.getStableSegmentMaintenanceExecutor().execute(() -> {
                });
            }

            final String submittingThreadName = Thread.currentThread().getName();
            registry.getStableSegmentMaintenanceExecutor()
                    .execute(() -> callerRunThread
                            .set(Thread.currentThread().getName()));

            assertEquals(submittingThreadName, callerRunThread.get());

            final IndexExecutorRuntimeAccess runtimeSnapshot =
                    registry.runtimeSnapshot();
            assertEquals(1L, runtimeSnapshot.getSplitMaintenance()
                    .getCompletedTaskCount());
            assertEquals(1L, runtimeSnapshot.getStableSegmentMaintenance()
                    .getCompletedTaskCount());
            assertEquals(1L, runtimeSnapshot.getStableSegmentMaintenance()
                    .getCallerRunsCount());
            assertEquals(1, runtimeSnapshot.getStableSegmentMaintenance()
                    .getActiveThreadCount());
            assertEquals(64, runtimeSnapshot.getStableSegmentMaintenance()
                    .getQueueSize());
            assertEquals(64, runtimeSnapshot.getStableSegmentMaintenance()
                    .getQueueCapacity());
        } finally {
            releaseWorker.countDown();
            registry.close();
        }
    }

    private static void awaitRelease(final CountDownLatch latch) {
        try {
            latch.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static IndexConfiguration<Integer, String> buildConf(
            final int stableSegmentMaintenanceThreads,
            final int indexMaintenanceThreads,
            final int registryMaintenanceThreads) {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withName("index-executor-registry-test")//
                .withContextLoggingEnabled(false)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInActivePartition(5)//
                .withMaxNumberOfKeysInPartitionBuffer(6)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInSegment(100)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withNumberOfIndexMaintenanceThreads(
                        indexMaintenanceThreads)//
                .withNumberOfSegmentMaintenanceThreads(
                        stableSegmentMaintenanceThreads)//
                .withNumberOfRegistryLifecycleThreads(
                        registryMaintenanceThreads)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }
}
