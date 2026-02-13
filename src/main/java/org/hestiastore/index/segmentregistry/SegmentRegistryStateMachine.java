package org.hestiastore.index.segmentregistry;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Atomic state holder for registry lifecycle gate transitions.
 * <p>
 * Registry layer maps gate states to operation outcomes:
 * {@code READY -> normal},
 * {@code FREEZE -> BUSY},
 * {@code CLOSED -> CLOSED},
 * {@code ERROR -> ERROR}.
 * {@code ERROR} is terminal.
 */
public final class SegmentRegistryStateMachine {

    private final AtomicReference<SegmentRegistryState> state = new AtomicReference<>(
            SegmentRegistryState.FREEZE);

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
     * @return true when FREEZE was entered from READY
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
     * @return true when state becomes CLOSED (or is already CLOSED),
     *         false when state is terminal ERROR
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
            if (current == SegmentRegistryState.READY
                    && state.compareAndSet(SegmentRegistryState.READY,
                            SegmentRegistryState.FREEZE)) {
                continue;
            }
            if (state.compareAndSet(SegmentRegistryState.FREEZE,
                    SegmentRegistryState.CLOSED)) {
                return true;
            }
        }
    }

    /**
     * Transitions from FREEZE to CLOSED.
     *
     * @return true when state becomes CLOSED
     */
    public boolean finishFreezeToClosed() {
        if (state.get() == SegmentRegistryState.CLOSED) {
            return true;
        }
        return state.compareAndSet(SegmentRegistryState.FREEZE,
                SegmentRegistryState.CLOSED);
    }

    /**
     * Marks the registry as failed.
     */
    public void fail() {
        state.set(SegmentRegistryState.ERROR);
    }
}
