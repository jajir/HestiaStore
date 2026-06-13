package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;

/**
 * Owns lifecycle and state collaborators behind one internal boundary so
 * {@link SegmentIndexImpl} can stay focused on API shape.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexSessionOwner<K, V> {

    private final SegmentIndexStateMachine stateMachine;
    private final IndexCloseCoordinator<K, V> closeCoordinator;

    /**
     * Creates the owner for lifecycle state and close coordination.
     *
     * @param stateMachine lifecycle state machine
     * @param closeCoordinator close sequence coordinator
     */
    SegmentIndexSessionOwner(
            final SegmentIndexStateMachine stateMachine,
            final IndexCloseCoordinator<K, V> closeCoordinator) {
        this.stateMachine = Vldtn.requireNonNull(stateMachine,
                "stateMachine");
        this.closeCoordinator = Vldtn.requireNonNull(closeCoordinator,
                "closeCoordinator");
    }

    void ensureOperational() {
        stateMachine.ensureOperational();
    }

    void close() {
        closeCoordinator.close();
    }

    SegmentIndexStateMachine stateMachine() {
        return stateMachine;
    }
}
