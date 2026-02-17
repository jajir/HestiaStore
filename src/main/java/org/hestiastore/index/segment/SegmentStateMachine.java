package org.hestiastore.index.segment;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Atomic state machine for segment lifecycle transitions.
 */
final class SegmentStateMachine {

    private final AtomicReference<SegmentState> state = new AtomicReference<>(
            SegmentState.READY);

    /**
     * Returns the current lifecycle state.
     *
     * @return current state
     */
    SegmentState getState() {
        return state.get();
    }

    /**
     * Attempts to enter an exclusive FREEZE state from READY.
     *
     * @return true when the transition succeeds
     */
    boolean tryEnterFreeze() {
        return state.compareAndSet(SegmentState.READY, SegmentState.FREEZE);
    }

    /**
     * Moves from FREEZE into MAINTENANCE_RUNNING.
     *
     * @return true when the transition succeeds
     */
    boolean enterMaintenanceRunning() {
        return state.compareAndSet(SegmentState.FREEZE,
                SegmentState.MAINTENANCE_RUNNING);
    }

    /**
     * Moves from MAINTENANCE_RUNNING back to FREEZE.
     *
     * @return true when the transition succeeds
     */
    boolean finishMaintenanceToFreeze() {
        return state.compareAndSet(SegmentState.MAINTENANCE_RUNNING,
                SegmentState.FREEZE);
    }

    /**
     * Moves from FREEZE back to READY.
     *
     * @return true when the transition succeeds
     */
    boolean finishFreezeToReady() {
        return state.compareAndSet(SegmentState.FREEZE, SegmentState.READY);
    }

    /**
     * Moves from FREEZE into CLOSED.
     *
     * @return true when the transition succeeds
     */
    boolean finishFreezeToClosed() {
        return state.compareAndSet(SegmentState.FREEZE, SegmentState.CLOSED);
    }

    /**
     * Forces the state to CLOSED regardless of current state.
     */
    void forceClosed() {
        state.set(SegmentState.CLOSED);
    }

    /**
     * Forces the state to ERROR regardless of current state.
     */
    void fail() {
        state.set(SegmentState.ERROR);
    }
}
