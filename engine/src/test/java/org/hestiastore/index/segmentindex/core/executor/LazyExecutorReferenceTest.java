package org.hestiastore.index.segmentindex.core.executor;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class LazyExecutorReferenceTest {

    @Test
    void createsExecutorOnlyOnFirstAccessAndReusesTheSameInstance() {
        final AtomicInteger createCount = new AtomicInteger();
        final LazyExecutorReference<ExecutorService> reference =
                new LazyExecutorReference<>(() -> {
                    createCount.incrementAndGet();
                    return Executors.newSingleThreadExecutor();
                });

        assertNull(reference.getIfCreated());

        final ExecutorService first = reference.get();
        final ExecutorService second = reference.get();

        assertSame(first, second);
        assertSame(first, reference.getIfCreated());
        org.junit.jupiter.api.Assertions.assertEquals(1, createCount.get());
        first.shutdownNow();
    }
}
