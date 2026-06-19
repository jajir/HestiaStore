package org.hestiastore.index.segmentindex.core.session;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;

/**
 * Runs index operations under shared operation tracking and readiness checks.
 */
final class SegmentIndexTrackedOperationRunner {

    private final SegmentIndexStateMachine stateMachine;
    private final SegmentIndexOperationGate operationGate;

    /**
     * Creates a tracked operation runner.
     *
     * @param stateMachine lifecycle state checked before each operation
     * @param operationGate operation gate that tracks active work
     */
    SegmentIndexTrackedOperationRunner(
            final SegmentIndexStateMachine stateMachine,
            final SegmentIndexOperationGate operationGate) {
        this.stateMachine = Vldtn.requireNonNull(stateMachine, "stateMachine");
        this.operationGate = Vldtn.requireNonNull(operationGate,
                "operationGate");
    }

    <T> T runTracked(final Supplier<T> operation) {
        final Supplier<T> nonNullOperation = Vldtn.requireNonNull(operation,
                "operation");
        return operationGate.trackOperation(() -> {
            stateMachine.ensureOperational();
            return nonNullOperation.get();
        });
    }

    void runTrackedVoid(final Runnable operation) {
        final Runnable nonNullOperation = Vldtn.requireNonNull(operation,
                "operation");
        runTracked(() -> {
            nonNullOperation.run();
            return null;
        });
    }
}
