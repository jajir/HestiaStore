package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class IndexOperationTrackerTest {

    @Test
    void awaitOperations_waitsForTrackedSyncTaskToFinish() throws Exception {
        final IndexOperationTracker tracker = new IndexOperationTracker();
        final CountDownLatch taskStarted = new CountDownLatch(1);
        final CountDownLatch releaseTask = new CountDownLatch(1);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final Future<?> trackedTask = executor.submit(() -> tracker
                    .runTracked(() -> {
                        taskStarted.countDown();
                        await(releaseTask);
                        return null;
                    }));
            assertTrue(taskStarted.await(1, TimeUnit.SECONDS));

            final Future<?> awaitTask = executor.submit(tracker::awaitOperations);
            assertThrows(java.util.concurrent.TimeoutException.class,
                    () -> awaitTask.get(100, TimeUnit.MILLISECONDS));

            releaseTask.countDown();
            trackedTask.get(1, TimeUnit.SECONDS);
            awaitTask.get(1, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void awaitOperations_throwsWhenCalledFromTrackedSyncOperation() {
        final IndexOperationTracker tracker = new IndexOperationTracker();

        final IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> tracker.runTracked(() -> {
                    tracker.awaitOperations();
                    return null;
                }));

        assertEquals("close() must not be called from an index operation.",
                thrown.getMessage());
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
