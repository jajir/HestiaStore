package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentAsyncExecutorTest {

    private SegmentAsyncExecutor executor;

    @BeforeEach
    void setUp() {
        executor = new SegmentAsyncExecutor(1, "segment-async-test");
    }

    @AfterEach
    void tearDown() {
        if (executor != null && !executor.wasClosed()) {
            executor.close();
        }
        executor = null;
    }

    @Test
    void constructorRejectsNonPositiveThreads() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentAsyncExecutor(0, "segment-async-test"));
        assertEquals("Property 'threads' must be greater than 0",
                ex.getMessage());
    }

    @Test
    void constructorRejectsEmptyPrefix() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentAsyncExecutor(1, ""));
        assertEquals("Property 'prefix' must not be empty.", ex.getMessage());
    }

    @Test
    void queueCapacityUsesMinimum() {
        assertEquals(64, executor.getQueueCapacity());
    }

    @Test
    void queueCapacityScalesWithThreads() {
        try (SegmentAsyncExecutor other = new SegmentAsyncExecutor(2,
                "segment-async-other")) {
            assertEquals(128, other.getQueueCapacity());
        }
    }

    @Test
    void usesThreadNamePrefix() throws Exception {
        final Future<String> future = executor.getExecutor()
                .submit(() -> Thread.currentThread().getName());
        final String name = future.get(1, TimeUnit.SECONDS);
        assertTrue(name.startsWith("segment-async-test-"));
    }

    @Test
    void closeShutsDownExecutor() {
        final SegmentAsyncExecutor closeExecutor = executor;
        executor = null;

        closeExecutor.close();

        assertTrue(closeExecutor.getExecutor().isShutdown());
        assertTrue(closeExecutor.getExecutor().isTerminated());
    }
}
