package org.hestiastore.index.segmentindex.core.session;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;

/**
 * Tracks foreground segment-index operations and waits for them during close.
 *
 * <p>
 * The gate tracks synchronous operations while they execute and exposes a wait
 * point used by close coordination. Lifecycle validation remains the caller's
 * responsibility and should be performed inside the tracked operation.
 * </p>
 */
public final class SegmentIndexOperationGate {

    private final Object operationMonitor = new Object();
    private int syncOperationsInFlight;
    private final ThreadLocal<Integer> syncOperationDepth = ThreadLocal
            .withInitial(() -> Integer.valueOf(0));

    private SegmentIndexOperationGate() {
    }

    /**
     * Creates the default operation gate.
     *
     * @return operation gate
     */
    static SegmentIndexOperationGate create() {
        return new SegmentIndexOperationGate();
    }

    /**
     * Executes a task while it is counted as an in-flight foreground operation.
     *
     * @param <T> task result type
     * @param task task to execute
     * @return task result
     */
    <T> T trackOperation(final Supplier<T> task) {
        final Supplier<T> nonNullTask = Vldtn.requireNonNull(task, "task");
        if (isInSyncOperation()) {
            return runWithSyncOperationContext(nonNullTask);
        }
        incrementSyncOperations();
        try {
            return runWithSyncOperationContext(nonNullTask);
        } finally {
            decrementSyncOperations();
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

    private <T> T runWithSyncOperationContext(final Supplier<T> task) {
        final int previousDepth = syncOperationDepth.get().intValue();
        syncOperationDepth.set(Integer.valueOf(previousDepth + 1));
        try {
            return task.get();
        } finally {
            syncOperationDepth.set(Integer.valueOf(previousDepth));
        }
    }

    private boolean isInSyncOperation() {
        return syncOperationDepth.get().intValue() > 0;
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
