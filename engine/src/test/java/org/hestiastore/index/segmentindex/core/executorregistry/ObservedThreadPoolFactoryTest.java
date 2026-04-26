package org.hestiastore.index.segmentindex.core.executorregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;

import org.junit.jupiter.api.Test;

class ObservedThreadPoolFactoryTest {

    @Test
    void createAbortingPoolTracksRejectedTasks() throws InterruptedException {
        final ObservedThreadPool pool = new ObservedThreadPoolFactory()
                .createAbortingPool(1, "indexMaintenanceThreads",
                        "reject-test-");
        final CountDownLatch blocker = new CountDownLatch(1);
        try {
            pool.executor().execute(() -> await(blocker));
            for (int i = 0; i < 64; i++) {
                pool.executor().execute(() -> await(blocker));
            }

            assertThrows(RejectedExecutionException.class,
                    () -> pool.executor().execute(() -> {
                    }));
            assertEquals(1L, pool.snapshot().getRejectedTaskCount());
        } finally {
            blocker.countDown();
            pool.executor().shutdownNow();
        }
    }

    @Test
    void createCallerRunsPoolTracksCallerRuns() throws InterruptedException {
        final ObservedThreadPool pool = new ObservedThreadPoolFactory()
                .createCallerRunsPool(1, "segmentMaintenanceThreads",
                        "caller-runs-test-");
        final CountDownLatch blocker = new CountDownLatch(1);
        try {
            pool.executor().execute(() -> await(blocker));
            for (int i = 0; i < 64; i++) {
                pool.executor().execute(() -> await(blocker));
            }

            pool.executor().execute(() -> {
            });

            assertEquals(1L, pool.snapshot().getCallerRunsCount());
        } finally {
            blocker.countDown();
            pool.executor().shutdownNow();
        }
    }

    @Test
    void daemonThreadFactoryCreatesNamedDaemonThreads() {
        final Thread thread = new ObservedThreadPoolFactory()
                .daemonThreadFactory("observed-thread-").newThread(() -> {
                });

        assertTrue(thread.isDaemon());
        assertTrue(thread.getName().startsWith("observed-thread-"));
    }

    private static void await(final CountDownLatch blocker) {
        try {
            blocker.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
