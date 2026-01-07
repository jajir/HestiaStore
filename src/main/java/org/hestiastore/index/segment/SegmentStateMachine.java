package org.hestiastore.index.segment;

import java.util.concurrent.atomic.AtomicReference;

final class SegmentStateMachine {

    private final AtomicReference<SegmentState> state = new AtomicReference<>(
            SegmentState.READY);

    SegmentState getState() {
        return state.get();
    }

    boolean tryEnterFreeze() {
        return state.compareAndSet(SegmentState.READY, SegmentState.FREEZE);
    }

    boolean enterMaintenanceRunning() {
        return state.compareAndSet(SegmentState.FREEZE,
                SegmentState.MAINTENANCE_RUNNING);
    }

    boolean finishMaintenanceToFreeze() {
        return state.compareAndSet(SegmentState.MAINTENANCE_RUNNING,
                SegmentState.FREEZE);
    }

    boolean finishFreezeToReady() {
        return state.compareAndSet(SegmentState.FREEZE, SegmentState.READY);
    }

    boolean closeFromReady() {
        return state.compareAndSet(SegmentState.READY, SegmentState.CLOSED);
    }

    void fail() {
        state.set(SegmentState.ERROR);
    }
}
