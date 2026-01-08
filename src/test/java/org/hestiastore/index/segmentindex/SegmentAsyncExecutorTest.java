package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

class SegmentAsyncExecutorTest {

    @Test
    void constructorRejectsNonPositiveThreadCount() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentAsyncExecutor(0, "segment"));
    }

    @Test
    void executorUsesNamedThreadsAndShutsDown() throws Exception {
        final SegmentAsyncExecutor executor = new SegmentAsyncExecutor(1,
                "segment-async-test");
        try {
            final AtomicReference<String> name = new AtomicReference<>();
            final Future<?> future = executor.getExecutor().submit(() -> {
                name.set(Thread.currentThread().getName());
            });
            future.get(1, TimeUnit.SECONDS);

            assertNotNull(name.get());
            assertTrue(name.get().startsWith("segment-async-test-"));
        } finally {
            executor.close();
        }
        assertTrue(executor.getExecutor().isShutdown());
    }
}
