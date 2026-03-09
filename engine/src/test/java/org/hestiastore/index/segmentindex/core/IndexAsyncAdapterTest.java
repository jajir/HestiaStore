package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexAsyncAdapterTest {

    @Mock
    private SegmentIndex<String, String> delegate;

    private ExecutorService executor;
    private IndexAsyncAdapter<String, String> adapter;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadExecutor(
                runnable -> new Thread(runnable, "test-index-worker-1"));
        adapter = new IndexAsyncAdapter<>(delegate, executor);
    }

    @AfterEach
    void tearDown() {
        if (adapter != null && !adapter.wasClosed()) {
            adapter.close();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void close_does_not_wait_for_inflight_async_operation() throws Exception {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        doAnswer(invocation -> {
            entered.countDown();
            if (!release.await(5, TimeUnit.SECONDS)) {
                throw new TimeoutException("Timed out waiting for release");
            }
            return "value";
        }).when(delegate).get("key");

        final CompletionStage<String> stage = adapter.getAsync("key");

        assertTrue(entered.await(5, TimeUnit.SECONDS),
                "Async operation did not start in time");

        final CompletableFuture<Void> closeFuture = CompletableFuture
                .runAsync(adapter::close);

        assertTrue(closeFuture.isDone()
                || closeFuture.get(500, TimeUnit.MILLISECONDS) == null,
                "close() should not block on an in-flight async operation");

        release.countDown();

        assertEquals("value",
                stage.toCompletableFuture().get(5, TimeUnit.SECONDS));
        closeFuture.get(5, TimeUnit.SECONDS);
        verify(delegate).close();
    }

    @Test
    void getAsync_runs_on_configured_executor_thread() throws Exception {
        doAnswer(invocation -> Thread.currentThread().getName()).when(delegate)
                .get("key");

        final String workerName = adapter.getAsync("key").toCompletableFuture()
                .get(5, TimeUnit.SECONDS);

        assertEquals("test-index-worker-1", workerName);
    }

    @Test
    void close_does_not_shutdown_external_executor() {
        adapter.close();

        assertFalse(executor.isShutdown());
        verify(delegate).close();
    }

    @Test
    void default_constructor_uses_dedicated_index_worker_executor()
            throws Exception {
        final IndexConfiguration<String, String> conf = IndexConfiguration
                .<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withName("async-adapter-test")//
                .withContextLoggingEnabled(false)//
                .withIndexWorkerThreadCount(1)//
                .build();
        when(delegate.getConfiguration()).thenReturn(conf);
        doAnswer(invocation -> Thread.currentThread().getName()).when(delegate)
                .get("key");

        try (IndexAsyncAdapter<String, String> localAdapter = new IndexAsyncAdapter<>(
                delegate)) {
            final String workerName = localAdapter.getAsync("key")
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);

            assertTrue(workerName.startsWith("index-worker-"));
        }
    }
}
