package org.hestiastore.index.segmentindex.core.session;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;

/**
 * Runs index operations under shared operation tracking and readiness checks.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexTrackedOperationRunner<K, V> {

    private final Runnable operationalGuard;
    private final IndexOperationTrackingAccess operationTracker;

    public SegmentIndexTrackedOperationRunner(
            final Runnable operationalGuard,
            final IndexOperationTrackingAccess operationTracker) {
        this.operationalGuard = Vldtn.requireNonNull(operationalGuard,
                "operationalGuard");
        this.operationTracker = Vldtn.requireNonNull(operationTracker,
                "operationTracker");
    }

    public <T> T runTracked(final Supplier<T> operation) {
        final Supplier<T> nonNullOperation = Vldtn.requireNonNull(operation,
                "operation");
        return operationTracker.runTracked(() -> {
            operationalGuard.run();
            return nonNullOperation.get();
        });
    }

    public void runTrackedVoid(final Runnable operation) {
        final Runnable nonNullOperation = Vldtn.requireNonNull(operation,
                "operation");
        runTracked(() -> {
            nonNullOperation.run();
            return null;
        });
    }
}
