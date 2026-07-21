package org.hestiastore.index.segmentindex.core.session;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Tracks foreground segment-index operations and waits for them during close.
 *
 * <p>
 * Foreground admission uses an atomic in-flight counter so ordinary operations
 * do not contend on the close-side drain mechanism. Lifecycle validation
 * remains the caller's responsibility and should be performed inside the
 * tracked operation. Callers must enter the closing state before waiting for
 * the counter to drain.
 * </p>
 */
public final class SessionOperationGate {

    private static final long INITIAL_WAIT_NANOS = TimeUnit.MICROSECONDS
            .toNanos(50);
    private static final long MAX_WAIT_NANOS = TimeUnit.MILLISECONDS
            .toNanos(1);

    private final AtomicInteger syncOperationsInFlight = new AtomicInteger();
    private final ThreadLocal<Integer> syncOperationDepth = ThreadLocal
            .withInitial(() -> 0);

    private SessionOperationGate() {
    }

    /**
     * Creates the default operation gate.
     *
     * @return operation gate
     */
    static SessionOperationGate create() {
        return new SessionOperationGate();
    }

    /**
     * Marks the current thread as running a foreground operation.
     */
    void beginOperation() {
        final int previousDepth = syncOperationDepth.get();
        if (previousDepth == 0) {
            incrementSyncOperations();
        }
        syncOperationDepth.set(previousDepth + 1);
    }

    /**
     * Marks the current thread as finished with a foreground operation.
     */
    void endOperation() {
        final int previousDepth = syncOperationDepth.get();
        if (previousDepth <= 0) {
            throw new IllegalStateException(
                    "No tracked index operation is active.");
        }
        final int nextDepth = previousDepth - 1;
        if (nextDepth == 0) {
            syncOperationDepth.remove();
            decrementSyncOperations();
        } else {
            syncOperationDepth.set(nextDepth);
        }
    }

    /**
     * Waits with bounded backoff until tracked foreground operations finish.
     * The caller must stop admitting operational work before invoking this
     * method.
     */
    void awaitOperationDrain() {
        if (isInSyncOperation()) {
            throw new IllegalStateException(
                    "close() must not be called from an index operation.");
        }
        long waitNanos = INITIAL_WAIT_NANOS;
        while (syncOperationsInFlight.get() > 0) {
            LockSupport.parkNanos(waitNanos);
            throwIfInterrupted();
            waitNanos = Math.min(MAX_WAIT_NANOS, waitNanos << 1);
        }
    }

    private boolean isInSyncOperation() {
        return syncOperationDepth.get() > 0;
    }

    private void incrementSyncOperations() {
        syncOperationsInFlight.incrementAndGet();
    }

    private void decrementSyncOperations() {
        syncOperationsInFlight.decrementAndGet();
    }

    private static void throwIfInterrupted() {
        if (Thread.interrupted()) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while waiting for tracked operations to finish.",
                    new InterruptedException());
        }
    }
}
