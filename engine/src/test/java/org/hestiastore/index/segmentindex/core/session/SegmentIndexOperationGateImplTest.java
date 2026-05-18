package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class SegmentIndexOperationGateImplTest {

    @Test
    void awaitOperationDrain_waitsForTrackedSyncTaskToFinish() throws Exception {
        final SegmentIndexOperationGateImpl gate =
                new SegmentIndexOperationGateImpl();
        final CountDownLatch taskStarted = new CountDownLatch(1);
        final CountDownLatch releaseTask = new CountDownLatch(1);
        final ExecutorService trackedExecutor = Executors
                .newSingleThreadExecutor();
        final ExecutorService waiterExecutor = Executors
                .newSingleThreadExecutor();
        try {
            final Future<?> trackedTask = trackedExecutor.submit(
                    () -> gate.trackOperation(() -> {
                        taskStarted.countDown();
                        await(releaseTask);
                        return null;
                    }));
            assertTrue(taskStarted.await(1, TimeUnit.SECONDS));

            final Future<?> awaitTask = waiterExecutor
                    .submit(gate::awaitOperationDrain);
            assertThrows(java.util.concurrent.TimeoutException.class,
                    () -> awaitTask.get(100, TimeUnit.MILLISECONDS));

            releaseTask.countDown();
            trackedTask.get(1, TimeUnit.SECONDS);
            awaitTask.get(1, TimeUnit.SECONDS);
        } finally {
            trackedExecutor.shutdownNow();
            waiterExecutor.shutdownNow();
        }
    }

    @Test
    void awaitOperationDrain_throwsWhenCalledFromTrackedSyncOperation() {
        final SegmentIndexOperationGateImpl gate =
                new SegmentIndexOperationGateImpl();

        final IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> gate.trackOperation(() -> {
                    gate.awaitOperationDrain();
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
