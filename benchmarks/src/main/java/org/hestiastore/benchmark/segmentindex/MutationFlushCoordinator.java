package org.hestiastore.benchmark.segmentindex;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Coordinates one aggregate periodic flush across concurrent benchmark
 * writers.
 */
final class MutationFlushCoordinator {

    private final AtomicInteger pendingMutations = new AtomicInteger();
    private final AtomicBoolean flushInProgress = new AtomicBoolean();

    /**
     * Records one mutation and claims the flush gate when the aggregate
     * threshold has been reached.
     *
     * @param flushBatchSize aggregate mutation count between flushes
     * @return true when the caller owns the flush gate
     */
    boolean recordAndTryStartFlush(final int flushBatchSize) {
        final int pending = pendingMutations.incrementAndGet();
        if (pending < flushBatchSize
                || !flushInProgress.compareAndSet(false, true)) {
            return false;
        }
        if (pendingMutations.getAndSet(0) >= flushBatchSize) {
            return true;
        }
        flushInProgress.set(false);
        return false;
    }

    /**
     * Releases the flush gate after the owning caller completes its flush.
     */
    void finishFlush() {
        flushInProgress.set(false);
    }

    /**
     * Clears mutations retained for the next periodic flush.
     */
    void reset() {
        pendingMutations.set(0);
    }
}
