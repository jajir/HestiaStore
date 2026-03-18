package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

class IndexAsyncOperationTrackerTest {

    @Test
    void awaitAsyncOperations_waitsForTrackedTaskToFinish() throws Exception {
        final IndexAsyncOperationTracker tracker = new IndexAsyncOperationTracker();
        final CountDownLatch taskStarted = new CountDownLatch(1);
        final CountDownLatch releaseTask = new CountDownLatch(1);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            tracker.runAsyncTracked(() -> {
                taskStarted.countDown();
                await(releaseTask);
                return "done";
            });
            assertTrue(taskStarted.await(1, TimeUnit.SECONDS));

            final Future<?> awaitTask = executor.submit(tracker::awaitAsyncOperations);
            assertThrows(java.util.concurrent.TimeoutException.class,
                    () -> awaitTask.get(100, TimeUnit.MILLISECONDS));

            releaseTask.countDown();
            awaitTask.get(1, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void awaitAsyncOperations_throwsWhenCalledFromTrackedAsyncOperation() {
        final IndexAsyncOperationTracker tracker = new IndexAsyncOperationTracker();
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        tracker.runAsyncTracked(() -> {
            try {
                tracker.awaitAsyncOperations();
            } catch (final Throwable e) {
                failure.set(e);
            }
            return null;
        }).toCompletableFuture().join();

        assertEquals(IllegalStateException.class,
                failure.get().getClass());
    }

    @Test
    void runAsyncTracked_propagatesTaskFailure() {
        final IndexAsyncOperationTracker tracker = new IndexAsyncOperationTracker();

        final CompletionException thrown = assertThrows(CompletionException.class,
                () -> tracker.runAsyncTracked(() -> {
                    throw new IllegalStateException("boom");
                }).toCompletableFuture().join());

        assertEquals(IllegalStateException.class, thrown.getCause().getClass());
    }

    private static void await(final CountDownLatch latch) {
        try {
            latch.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while waiting for tracked task release.", e);
        }
    }
}
