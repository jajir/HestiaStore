package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Retains actual merge jobs in the real executor queue until explicitly run.
 * Production failure cleanup therefore clears the same queue that is tested.
 */
final class SenkuManualWorkerExecutor extends ThreadPoolExecutor {

    SenkuManualWorkerExecutor() {
        super(1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(8));
    }

    /** {@inheritDoc} */
    @Override
    public void execute(final Runnable action) {
        if (isShutdown() || !getQueue().offer(action)) {
            throw new RejectedExecutionException(
                    "Manual worker cannot accept a job.");
        }
    }

    /** Executes exactly one previously submitted production merge job. */
    void runNextTask() {
        final Runnable task = getQueue().poll();
        assertNotNull(task, "Expected a submitted merge job.");
        task.run();
    }

    boolean hasPendingTasks() {
        return !getQueue().isEmpty();
    }
}
