package org.hestiastore.index.segmentindex.core.split;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mutable coordination state shared by the policy scheduler and worker tasks.
 */
final class SplitWorkerState {

    private final AtomicBoolean fullScanRequested =
            new AtomicBoolean(false);
    private final AtomicBoolean tickScheduled = new AtomicBoolean(false);
    private final AtomicInteger activeWorkerCount = new AtomicInteger();

    void markFullScanRequested() {
        fullScanRequested.set(true);
    }

    boolean consumeFullScanRequested() {
        return fullScanRequested.getAndSet(false);
    }

    boolean isFullScanRequested() {
        return fullScanRequested.get();
    }

    int reserveWorkers(final int maxWorkers) {
        final int previous = activeWorkerCount.getAndUpdate(
                current -> Math.max(current, maxWorkers));
        return Math.max(0, maxWorkers - previous);
    }

    void markWorkerFinished() {
        activeWorkerCount.decrementAndGet();
    }

    boolean isWorkerActive() {
        return activeWorkerCount.get() > 0;
    }

    boolean tryMarkTickScheduled() {
        return tickScheduled.compareAndSet(false, true);
    }

    void clearTickScheduled() {
        tickScheduled.set(false);
    }

    void clearPendingWork() {
        fullScanRequested.set(false);
    }

    boolean hasPendingWork() {
        return fullScanRequested.get();
    }
}
