package org.hestiastore.index.segmentindex.core.session;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;

/**
 * Runs index operations under shared operation tracking and readiness checks.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexTrackedOperationRunner<K, V> {

    private final SegmentIndexStateMachine stateMachine;
    private final SegmentIndexOperationGate operationGate;

    public SegmentIndexTrackedOperationRunner(
            final SegmentIndexStateMachine stateMachine,
            final SegmentIndexOperationGate operationGate) {
        this.stateMachine = Vldtn.requireNonNull(stateMachine, "stateMachine");
        this.operationGate = Vldtn.requireNonNull(operationGate,
                "operationGate");
    }

    public <T> T runTracked(final Supplier<T> operation) {
        final Supplier<T> nonNullOperation = Vldtn.requireNonNull(operation,
                "operation");
        return operationGate.trackOperation(() -> {
            stateMachine.ensureOperational();
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
