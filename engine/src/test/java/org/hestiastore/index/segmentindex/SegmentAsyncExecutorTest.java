package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentAsyncExecutorTest {

    private IndexExecutorRegistry registry;
    private SegmentAsyncExecutor executor;

    @BeforeEach
    void setUp() {
        registry = new IndexExecutorRegistry(1, 1, 1, 1);
        executor = new SegmentAsyncExecutor(registry.getSegmentMaintenanceExecutor());
    }

    @AfterEach
    void tearDown() {
        if (executor != null && !executor.wasClosed()) {
            executor.close();
        }
        if (registry != null && !registry.wasClosed()) {
            registry.close();
        }
        executor = null;
        registry = null;
    }

    @Test
    void constructorRejectsNullExecutor() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentAsyncExecutor(null));
        assertEquals("Property 'executor' must not be null.",
                ex.getMessage());
    }

    @Test
    void queueCapacityUsesMinimum() {
        assertEquals(64, executor.getQueueCapacity());
    }

    @Test
    void queueCapacityScalesWithThreads() {
        try (IndexExecutorRegistry otherRegistry = new IndexExecutorRegistry(1,
                2, 1, 1);
                SegmentAsyncExecutor other = new SegmentAsyncExecutor(
                        otherRegistry.getSegmentMaintenanceExecutor())) {
            assertEquals(128, other.getQueueCapacity());
        }
    }

    @Test
    void usesRegistryThreadNamePrefix() throws Exception {
        final Future<String> future = executor.getExecutor()
                .submit(() -> Thread.currentThread().getName());
        final String name = future.get(1, TimeUnit.SECONDS);
        assertTrue(name.startsWith("segment-maintenance-"));
    }

    @Test
    void closeDoesNotShutdownUnderlyingExecutor() {
        final SegmentAsyncExecutor closeExecutor = executor;
        executor = null;

        closeExecutor.close();

        assertFalse(closeExecutor.getExecutor().isShutdown());
    }
}
