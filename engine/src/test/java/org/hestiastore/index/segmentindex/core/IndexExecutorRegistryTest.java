package org.hestiastore.index.segmentindex.core;

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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexConfiguration;
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
        final IndexConfiguration<Integer, String> conf = buildConf(2, 3, 4, 5);
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(conf);
        try {
            assertNotNull(registry.getIoExecutor());
            assertNotNull(registry.getIndexMaintenanceExecutor());
            assertNotNull(registry.getSegmentMaintenanceExecutor());
            assertNotNull(registry.getRegistryMaintenanceExecutor());
        } finally {
            registry.close();
        }
    }

    @Test
    void configurationConstructorRejectsNonPositiveIoThreads() {
        final IndexConfiguration<Integer, String> conf = buildConf(0, 1, 1, 1);
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IndexExecutorRegistry(conf));
        assertEquals("Property 'ioThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void constructorRejectsNonPositiveIoThreads() {
        final IndexConfiguration<Integer, String> conf = buildConf(0, 1, 1, 1);
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IndexExecutorRegistry(conf));
        assertEquals("Property 'ioThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void constructorRejectsNonPositiveSegmentMaintenanceThreads() {
        final IndexConfiguration<Integer, String> conf = buildConf(1, 0, 1, 1);
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IndexExecutorRegistry(conf));
        assertEquals(
                "Property 'segmentMaintenanceThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void constructorRejectsNonPositiveIndexMaintenanceThreads() {
        final IndexConfiguration<Integer, String> conf = buildConf(1, 1, 0, 1);
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IndexExecutorRegistry(conf));
        assertEquals(
                "Property 'indexMaintenanceThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void constructorRejectsNonPositiveRegistryMaintenanceThreads() {
        final IndexConfiguration<Integer, String> conf = buildConf(1, 1, 1, 0);
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IndexExecutorRegistry(conf));
        assertEquals(
                "Property 'registryMaintenanceThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void closeShutsDownAllExecutors() {
        final IndexConfiguration<Integer, String> conf = buildConf(1, 1, 1, 1);
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(conf);
        final ExecutorService io = registry.getIoExecutor();
        final ExecutorService indexMaintenance = registry
                .getIndexMaintenanceExecutor();
        final ExecutorService segmentMaintenance = registry
                .getSegmentMaintenanceExecutor();
        final ExecutorService registryMaintenance = registry
                .getRegistryMaintenanceExecutor();

        registry.close();

        assertTrue(io.isShutdown());
        assertTrue(indexMaintenance.isShutdown());
        assertTrue(segmentMaintenance.isShutdown());
        assertTrue(registryMaintenance.isShutdown());
    }

    @Test
    void gettersRejectCallsAfterClose() {
        final IndexConfiguration<Integer, String> conf = buildConf(1, 1, 1, 1);
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(conf);
        registry.close();

        assertThrows(IllegalStateException.class, registry::getIoExecutor);
        assertThrows(IllegalStateException.class,
                registry::getIndexMaintenanceExecutor);
        assertThrows(IllegalStateException.class,
                registry::getSegmentMaintenanceExecutor);
        assertThrows(IllegalStateException.class,
                registry::getRegistryMaintenanceExecutor);
    }

    @Test
    void gettersReturnSameExecutorInstances() {
        final IndexConfiguration<Integer, String> conf = buildConf(1, 1, 1, 1);
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(conf);
        try {
            assertSame(registry.getIoExecutor(), registry.getIoExecutor());
            assertSame(registry.getIndexMaintenanceExecutor(),
                    registry.getIndexMaintenanceExecutor());
            assertSame(registry.getSegmentMaintenanceExecutor(),
                    registry.getSegmentMaintenanceExecutor());
            assertSame(registry.getRegistryMaintenanceExecutor(),
                    registry.getRegistryMaintenanceExecutor());
        } finally {
            registry.close();
        }
    }

    @Test
    void executorsUseExpectedThreadNamesAndDaemonThreads()
            throws InterruptedException, ExecutionException {
        final IndexConfiguration<Integer, String> conf = buildConf(1, 1, 1, 1);
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(conf);
        try {
            final String ioName = registry.getIoExecutor()
                    .submit(() -> Thread.currentThread().getName()).get();
            final String indexMaintenanceName = registry
                    .getIndexMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().getName()).get();
            final String segmentMaintenanceName = registry
                    .getSegmentMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().getName()).get();
            final String registryMaintenanceName = registry
                    .getRegistryMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().getName()).get();

            final boolean ioDaemon = registry.getIoExecutor()
                    .submit(() -> Thread.currentThread().isDaemon()).get();
            final boolean indexMaintenanceDaemon = registry
                    .getIndexMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().isDaemon()).get();
            final boolean segmentMaintenanceDaemon = registry
                    .getSegmentMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().isDaemon()).get();
            final boolean registryMaintenanceDaemon = registry
                    .getRegistryMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().isDaemon()).get();

            assertTrue(ioName.startsWith("index-io-"));
            assertTrue(indexMaintenanceName.startsWith("index-maintenance-"));
            assertTrue(segmentMaintenanceName.startsWith("segment-maintenance-"));
            assertTrue(
                    registryMaintenanceName.startsWith("registry-maintenance-"));
            assertTrue(ioDaemon);
            assertTrue(indexMaintenanceDaemon);
            assertTrue(segmentMaintenanceDaemon);
            assertTrue(registryMaintenanceDaemon);
        } finally {
            registry.close();
        }
    }

    @Test
    void ioExecutorUsesConfiguredThreadCount() throws Exception {
        final int ioThreads = 4;
        final IndexConfiguration<Integer, String> conf = buildConf(ioThreads, 1,
                1, 1);
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(conf);
        final CountDownLatch started = new CountDownLatch(ioThreads);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            final List<java.util.concurrent.Future<String>> futures = java.util.stream.IntStream
                    .range(0, ioThreads)
                    .mapToObj(i -> registry.getIoExecutor().submit(() -> {
                        started.countDown();
                        release.await();
                        return Thread.currentThread().getName();
                    }))
                    .toList();
            assertTrue(started.await(2, TimeUnit.SECONDS),
                    "IO executor did not start all configured workers.");
            release.countDown();
            final Set<String> threadNames = futures.stream().map(future -> {
                try {
                    return future.get();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "Interrupted while collecting IO worker names.", e);
                } catch (final ExecutionException e) {
                    throw new IllegalStateException(
                            "Failed while collecting IO worker names.",
                            e.getCause());
                }
            }).collect(Collectors.toSet());
            assertEquals(ioThreads, threadNames.size());
        } finally {
            release.countDown();
            registry.close();
        }
    }

    private static IndexConfiguration<Integer, String> buildConf(
            final int ioThreads, final int segmentMaintenanceThreads,
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
                .withMaxNumberOfKeysInSegmentWriteCache(5)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(6)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInSegment(100)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withIndexWorkerThreadCount(1)//
                .withNumberOfIndexMaintenanceThreads(
                        indexMaintenanceThreads)//
                .withNumberOfIoThreads(ioThreads)//
                .withNumberOfSegmentIndexMaintenanceThreads(
                        segmentMaintenanceThreads)//
                .withNumberOfRegistryLifecycleThreads(
                        registryMaintenanceThreads)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }
}
