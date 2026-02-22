package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;

class IndexExecutorRegistryTest {

    @Test
    void constructorRejectsNonPositiveIoThreads() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IndexExecutorRegistry(0));
        assertEquals("Property 'ioThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void constructorRejectsNonPositiveSegmentMaintenanceThreads() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IndexExecutorRegistry(1, 0, 1, 1));
        assertEquals(
                "Property 'segmentMaintenanceThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void constructorRejectsNonPositiveSegmentThreads() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IndexExecutorRegistry(1, 1, 0, 1));
        assertEquals("Property 'segmentThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void constructorRejectsNonPositiveRegistryMaintenanceThreads() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IndexExecutorRegistry(1, 1, 1, 0));
        assertEquals(
                "Property 'registryMaintenanceThreads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void closeShutsDownAllExecutors() {
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(1, 1,
                1, 1);
        final ExecutorService io = registry.getIoExecutor();
        final ExecutorService segment = registry.getSegmentExecutor();
        final ExecutorService segmentMaintenance = registry
                .getSegmentMaintenanceExecutor();
        final ExecutorService registryMaintenance = registry
                .getRegistryMaintenanceExecutor();

        registry.close();

        assertTrue(io.isShutdown());
        assertTrue(segment.isShutdown());
        assertTrue(segmentMaintenance.isShutdown());
        assertTrue(registryMaintenance.isShutdown());
    }

    @Test
    void gettersRejectCallsAfterClose() {
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(1, 1,
                1, 1);
        registry.close();

        assertThrows(IllegalStateException.class, registry::getIoExecutor);
        assertThrows(IllegalStateException.class, registry::getSegmentExecutor);
        assertThrows(IllegalStateException.class,
                registry::getSegmentMaintenanceExecutor);
        assertThrows(IllegalStateException.class,
                registry::getRegistryMaintenanceExecutor);
    }

    @Test
    void gettersReturnSameExecutorInstances() {
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(1, 1,
                1, 1);
        try {
            assertSame(registry.getIoExecutor(), registry.getIoExecutor());
            assertSame(registry.getSegmentExecutor(),
                    registry.getSegmentExecutor());
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
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(1, 1,
                1, 1);
        try {
            final String ioName = registry.getIoExecutor()
                    .submit(() -> Thread.currentThread().getName()).get();
            final String segmentName = registry.getSegmentExecutor()
                    .submit(() -> Thread.currentThread().getName()).get();
            final String segmentMaintenanceName = registry
                    .getSegmentMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().getName()).get();
            final String registryMaintenanceName = registry
                    .getRegistryMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().getName()).get();

            final boolean ioDaemon = registry.getIoExecutor()
                    .submit(() -> Thread.currentThread().isDaemon()).get();
            final boolean segmentDaemon = registry.getSegmentExecutor()
                    .submit(() -> Thread.currentThread().isDaemon()).get();
            final boolean segmentMaintenanceDaemon = registry
                    .getSegmentMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().isDaemon()).get();
            final boolean registryMaintenanceDaemon = registry
                    .getRegistryMaintenanceExecutor()
                    .submit(() -> Thread.currentThread().isDaemon()).get();

            assertTrue(ioName.startsWith("index-io-"));
            assertTrue(segmentName.startsWith("segment-"));
            assertTrue(segmentMaintenanceName.startsWith("segment-maintenance-"));
            assertTrue(
                    registryMaintenanceName.startsWith("registry-maintenance-"));
            assertTrue(ioDaemon);
            assertTrue(segmentDaemon);
            assertTrue(segmentMaintenanceDaemon);
            assertTrue(registryMaintenanceDaemon);
        } finally {
            registry.close();
        }
    }
}
