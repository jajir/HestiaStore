package org.hestiastore.index.segmentregistry;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Simple state holder for registry lifecycle transitions.
 */
public final class SegmentRegistryGate {

    private final AtomicReference<SegmentRegistryState> state = new AtomicReference<>(
            SegmentRegistryState.READY);

    /**
     * Returns the current registry state.
     *
     * @return registry state
     */
    public SegmentRegistryState getState() {
        return state.get();
    }

    /**
     * Attempts to enter the FREEZE state.
     *
     * @return true when FREEZE was entered
     */
    public boolean tryEnterFreeze() {
        return state.compareAndSet(SegmentRegistryState.READY,
                SegmentRegistryState.FREEZE);
    }

    /**
     * Transitions from FREEZE back to READY.
     *
     * @return true when the transition succeeds
     */
    public boolean finishFreezeToReady() {
        return state.compareAndSet(SegmentRegistryState.FREEZE,
                SegmentRegistryState.READY);
    }

    /**
     * Marks the registry closed.
     *
     * @return true when closed or already closed; false when in ERROR
     */
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

    /**
     * Marks the registry as failed.
     */
    public void fail() {
        state.set(SegmentRegistryState.ERROR);
    }
}
