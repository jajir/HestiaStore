package org.hestiastore.index.segmentregistry;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Simple state holder for registry lifecycle transitions.
 */
public final class SegmentRegistryGate {

    private final AtomicReference<SegmentRegistryState> state = new AtomicReference<>(
            SegmentRegistryState.READY);

    public SegmentRegistryState getState() {
        return state.get();
    }

    public boolean tryEnterFreeze() {
        return state.compareAndSet(SegmentRegistryState.READY,
                SegmentRegistryState.FREEZE);
    }

    public boolean finishFreezeToReady() {
        return state.compareAndSet(SegmentRegistryState.FREEZE,
                SegmentRegistryState.READY);
    }

    public boolean close() {
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

    public void fail() {
        state.set(SegmentRegistryState.ERROR);
    }
}
