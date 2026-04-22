package org.hestiastore.index.segmentindex.core.routing;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.routing.IndexOperationTrackingAccess;
import org.hestiastore.index.segmentindex.core.session.state.IndexState;

/**
 * Runs index operations under shared operation tracking and readiness checks.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexTrackedOperationRunner<K, V> {

    private final Supplier<IndexState<K, V>> indexStateSupplier;
    private final IndexOperationTrackingAccess operationTracker;

    public SegmentIndexTrackedOperationRunner(
            final Supplier<IndexState<K, V>> indexStateSupplier,
            final IndexOperationTrackingAccess operationTracker) {
        this.indexStateSupplier = Vldtn.requireNonNull(indexStateSupplier,
                "indexStateSupplier");
        this.operationTracker = Vldtn.requireNonNull(operationTracker,
                "operationTracker");
    }

    public <T> T runTracked(final Supplier<T> operation) {
        final Supplier<T> nonNullOperation = Vldtn.requireNonNull(operation,
                "operation");
        return operationTracker.runTracked(() -> {
            indexStateSupplier.get().tryPerformOperation();
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
