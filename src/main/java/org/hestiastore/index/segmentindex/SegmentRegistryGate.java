package org.hestiastore.index.segmentindex;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Simple state holder for registry lifecycle transitions.
 */
final class SegmentRegistryGate {

    private final AtomicReference<SegmentRegistryState> state = new AtomicReference<>(
            SegmentRegistryState.READY);

    SegmentRegistryState getState() {
        return state.get();
    }

    boolean tryEnterFreeze() {
        return state.compareAndSet(SegmentRegistryState.READY,
                SegmentRegistryState.FREEZE);
    }

    boolean finishFreezeToReady() {
        return state.compareAndSet(SegmentRegistryState.FREEZE,
                SegmentRegistryState.READY);
    }

    boolean close() {
        while (true) {
            final SegmentRegistryState current = state.get();
            if (current == SegmentRegistryState.ERROR) {
                return false;
            }
            if (current == SegmentRegistryState.CLOSED) {
                return true;
            }
            if (state.compareAndSet(current, SegmentRegistryState.CLOSED)) {
                return true;
            }
        }
    }

    void fail() {
        state.set(SegmentRegistryState.ERROR);
    }
}
