package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class IndexAsyncExecutorTest {

    private IndexAsyncExecutor asyncExecutor;

    @AfterEach
    void tearDown() {
        if (asyncExecutor != null && !asyncExecutor.wasClosed()) {
            asyncExecutor.close();
        }
    }

    @Test
    void runAsync_usesDedicatedIndexWorkerThread() throws Exception {
        asyncExecutor = new IndexAsyncExecutor(buildConf());

        final String workerName = asyncExecutor
                .runAsync(() -> Thread.currentThread().getName())
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertTrue(workerName.startsWith("index-worker-"));
    }

    @Test
    void close_doesNotWaitForInFlightAsyncTask() throws Exception {
        asyncExecutor = new IndexAsyncExecutor(buildConf());
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        final CompletableFuture<String> stage = asyncExecutor.runAsync(() -> {
            entered.countDown();
            await(release);
            return "done";
        }).toCompletableFuture();

        assertTrue(entered.await(5, TimeUnit.SECONDS),
                "Async operation did not start in time");

        final CompletableFuture<Void> closeFuture = CompletableFuture
                .runAsync(asyncExecutor::close);

        assertDoesNotThrow(() -> closeFuture.get(500, TimeUnit.MILLISECONDS),
                "close() should not block on an in-flight async operation");

        release.countDown();
        assertEquals("done", stage.get(5, TimeUnit.SECONDS));
    }

    @Test
    void close_throwsWhenCalledFromAsyncTask() {
        asyncExecutor = new IndexAsyncExecutor(buildConf());
        final CompletableFuture<?> closeFromAsyncTask = asyncExecutor
                .runAsync(() -> {
                    asyncExecutor.close();
                    return null;
                }).toCompletableFuture();

        final CompletionException thrown = assertThrows(CompletionException.class,
                closeFromAsyncTask::join);

        assertEquals(IllegalStateException.class, thrown.getCause().getClass());
    }

    private IndexConfiguration<String, String> buildConf() {
        return IndexConfiguration.<String, String>builder()
                .withKeyClass(String.class)
                .withValueClass(String.class)
                .withName("index-async-executor-test")
                .withContextLoggingEnabled(false)
                .withIndexWorkerThreadCount(1)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }

    private static void await(final CountDownLatch latch) {
        try {
            latch.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while waiting for async task release.", e);
        }
    }
}
