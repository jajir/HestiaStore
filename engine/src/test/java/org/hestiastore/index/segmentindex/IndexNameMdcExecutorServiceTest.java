package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class IndexNameMdcExecutorServiceTest {

    private ExecutorService delegate;

    @AfterEach
    void tearDown() {
        if (delegate != null) {
            delegate.shutdownNow();
            delegate = null;
        }
        MDC.clear();
    }

    @Test
    void constructorRejectsNullIndexName() {
        delegate = Executors.newSingleThreadExecutor();
        assertThrows(IllegalArgumentException.class,
                () -> new IndexNameMdcExecutorService(null, delegate));
    }

    @Test
    void setsIndexNameForTaskAndRestoresPreviousValue()
            throws InterruptedException, ExecutionException {
        delegate = Executors.newSingleThreadExecutor();

        final Future<?> seed = delegate.submit(
                () -> MDC.put("index.name", "previous"));
        seed.get();

        final IndexNameMdcExecutorService executor = new IndexNameMdcExecutorService(
                "idx", delegate);
        final Future<String> mdcInWrappedTask = executor
                .submit(() -> MDC.get("index.name"));
        assertEquals("idx", mdcInWrappedTask.get());

        final Future<String> mdcAfterWrappedTask = delegate
                .submit(() -> MDC.get("index.name"));
        assertEquals("previous", mdcAfterWrappedTask.get());
    }

    @Test
    void shutdownDelegatesToWrappedExecutor() throws Exception {
        delegate = Executors.newSingleThreadExecutor();
        final IndexNameMdcExecutorService executor = new IndexNameMdcExecutorService(
                "idx", delegate);

        executor.shutdown();

        executor.awaitTermination(1, TimeUnit.SECONDS);
        assertTrue(delegate.isShutdown());
    }
}
