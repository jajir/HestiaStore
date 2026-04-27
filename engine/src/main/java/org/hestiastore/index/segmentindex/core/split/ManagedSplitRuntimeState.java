package org.hestiastore.index.segmentindex.core.split;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Tracks the managed lifecycle of the split runtime independently from the
 * host index state supplier.
 */
final class ManagedSplitRuntimeState {

    private final AtomicReference<State> state = new AtomicReference<>(
            State.OPENING);

    void markRunning() {
        transition(State.OPENING, State.RUNNING,
                "become RUNNING after construction");
    }

    void requireRunning(final String action) {
        final State current = state.get();
        if (current != State.RUNNING) {
            throw new IllegalStateException(String.format(
                    "Split runtime cannot %s while %s.", action, current));
        }
    }

    boolean beginClose() {
        while (true) {
            final State current = state.get();
            if (current == State.CLOSED) {
                return false;
            }
            if (current == State.CLOSING) {
                return false;
            }
            if (current == State.OPENING) {
                throw new IllegalStateException(
                        "Split runtime cannot close while OPENING.");
            }
            if (state.compareAndSet(State.RUNNING, State.CLOSING)) {
                return true;
            }
        }
    }

    void markClosed() {
        while (true) {
            final State current = state.get();
            if (current == State.CLOSED) {
                return;
            }
            if (current != State.CLOSING) {
                throw new IllegalStateException(String.format(
                        "Split runtime cannot become CLOSED from %s.",
                        current));
            }
            if (state.compareAndSet(State.CLOSING, State.CLOSED)) {
                return;
            }
        }
    }

    private void transition(final State expected, final State next,
            final String action) {
        if (!state.compareAndSet(expected, next)) {
            throw new IllegalStateException(String.format(
                    "Split runtime cannot %s while %s.", action, state.get()));
        }
    }

    enum State {
        OPENING,
        RUNNING,
        CLOSING,
        CLOSED
    }
}
