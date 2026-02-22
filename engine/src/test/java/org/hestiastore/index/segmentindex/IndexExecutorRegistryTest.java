package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ExecutorService;

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
        final ExecutorService segmentMaintenance = registry
                .getSegmentMaintenanceExecutor();
        final ExecutorService registryMaintenance = registry
                .getRegistryMaintenanceExecutor();

        registry.close();

        assertTrue(io.isShutdown());
        assertTrue(segmentMaintenance.isShutdown());
        assertTrue(registryMaintenance.isShutdown());
    }

    @Test
    void gettersRejectCallsAfterClose() {
        final IndexExecutorRegistry registry = new IndexExecutorRegistry(1, 1,
                1, 1);
        registry.close();

        assertThrows(IllegalStateException.class, registry::getIoExecutor);
        assertThrows(IllegalStateException.class,
                registry::getSegmentMaintenanceExecutor);
        assertThrows(IllegalStateException.class,
                registry::getRegistryMaintenanceExecutor);
    }
}
