package org.hestiastore.index.segmentindex.core.session;

/**
 * Tracks foreground segment-index operations and waits for them during close.
 *
 * <p>
 * The gate tracks synchronous operations while they execute and exposes a wait
 * point used by close coordination. Lifecycle validation remains the caller's
 * responsibility and should be performed inside the tracked operation.
 * </p>
 */
public final class SessionOperationGate {

    private final Object operationMonitor = new Object();
    private int syncOperationsInFlight;
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
     * Waits until currently tracked foreground operations finish.
     */
    void awaitOperationDrain() {
        if (isInSyncOperation()) {
            throw new IllegalStateException(
                    "close() must not be called from an index operation.");
        }
        synchronized (operationMonitor) {
            while (syncOperationsInFlight > 0) {
                try {
                    operationMonitor.wait();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "Interrupted while waiting for tracked operations to finish.",
                            e);
                }
            }
        }
    }

    private boolean isInSyncOperation() {
        return syncOperationDepth.get() > 0;
    }

    private void incrementSyncOperations() {
        synchronized (operationMonitor) {
            syncOperationsInFlight++;
        }
    }

    private void decrementSyncOperations() {
        synchronized (operationMonitor) {
            syncOperationsInFlight--;
            operationMonitor.notifyAll();
        }
    }
}
