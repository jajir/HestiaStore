package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Captures real coordinator tasks in submission order without starting any
 * executor thread. Only the finish caller runs concurrently with the test.
 */
final class SenkuManualControlExecutor extends ScheduledThreadPoolExecutor {

    private final BlockingQueue<Runnable> pending = new LinkedBlockingQueue<>();
    private final SenkuManualScheduledFuture periodicFuture = new SenkuManualScheduledFuture();
    private Runnable periodicScan;

    SenkuManualControlExecutor() {
        super(1);
    }

    /** {@inheritDoc} */
    @Override
    public void execute(final Runnable action) {
        if (isShutdown()) {
            throw new RejectedExecutionException(
                    "Manual control is shut down.");
        }
        pending.add(action);
    }

    /** Records the scan instead of scheduling a timing-dependent task. */
    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable action,
            final long initialDelay, final long delay, final TimeUnit unit) {
        periodicScan = action;
        return periodicFuture;
    }

    /** Executes one periodic scan unless production code has cancelled it. */
    void runPeriodicScan() {
        if (!periodicFuture.isCancelled()) {
            periodicScan.run();
        }
    }

    /**
     * Waits only for submission by the finish caller, never for a time-based
     * schedule. The timeout is a deadlock watchdog, not a synchronization step.
     *
     * @return next submitted control task
     * @throws InterruptedException when interrupted while waiting
     */
    Runnable takeTask() throws InterruptedException {
        final Runnable task = pending.poll(10, TimeUnit.SECONDS);
        assertNotNull(task, "Expected the next submitted control task.");
        return task;
    }

    boolean hasPendingTasks() {
        return !pending.isEmpty();
    }

    boolean isPeriodicScanCancelled() {
        return periodicFuture.isCancelled();
    }
}
