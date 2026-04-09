package org.hestiastore.index.segmentindex.core;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;

/**
 * Tracks in-flight synchronous index operations and provides a close-safe
 * waiting point.
 */
final class IndexOperationTracker {

    private final Object operationMonitor = new Object();
    private int syncOperationsInFlight;
    private final ThreadLocal<Integer> syncOperationDepth = ThreadLocal
            .withInitial(() -> Integer.valueOf(0));

    <T> T runTracked(final Supplier<T> task) {
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

    void awaitOperations() {
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
