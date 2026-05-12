package org.hestiastore.index.segmentindex.core;

import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * Atomic state machine for SegmentIndex lifecycle transitions.
 */
public final class SegmentIndexStateMachine {

    private final AtomicReference<SegmentIndexState> state =
            new AtomicReference<>(SegmentIndexState.OPENING);
    private volatile Throwable failure;

    /**
     * Returns the current SegmentIndex state.
     *
     * @return current state
     */
    public SegmentIndexState getState() {
        return state.get();
    }

    /**
     * Moves the index from OPENING to READY.
     */
    public void markReady() {
        if (state.compareAndSet(SegmentIndexState.OPENING,
                SegmentIndexState.READY)) {
            return;
        }
        throw markReadyFailure(state.get());
    }

    /**
     * Starts the close transition from READY to CLOSING.
     */
    public void beginClose() {
        if (state.compareAndSet(SegmentIndexState.READY,
                SegmentIndexState.CLOSING)) {
            return;
        }
        final SegmentIndexState current = state.get();
        if (current == SegmentIndexState.ERROR) {
            return;
        }
        throw beginCloseFailure(current);
    }

    /**
     * Completes the close transition from CLOSING to CLOSED.
     */
    public void completeClose() {
        if (state.compareAndSet(SegmentIndexState.CLOSING,
                SegmentIndexState.CLOSED)) {
            return;
        }
        final SegmentIndexState current = state.get();
        if (current == SegmentIndexState.CLOSED
                || current == SegmentIndexState.ERROR) {
            return;
        }
        throw completeCloseFailure(current);
    }

    /**
     * Marks a returned index as failed after an unrecoverable runtime failure
     * and preserves the first failure cause.
     *
     * @param failureCause original runtime failure
     */
    public synchronized void markRuntimeFailure(final Throwable failureCause) {
        final Throwable nonNullFailure = Vldtn.requireNonNull(failureCause,
                "failure");
        if (state.get() == SegmentIndexState.ERROR) {
            return;
        }
        failure = nonNullFailure;
        state.set(SegmentIndexState.ERROR);
    }

    /**
     * Validates that index operations are currently allowed.
     */
    public void ensureOperational() {
        final SegmentIndexState current = state.get();
        switch (current) {
            case READY:
                return;
            case OPENING:
                throw new IllegalStateException(
                        "Can't perform operation while index is opening.");
            case CLOSING:
                throw new IllegalStateException(
                        "Can't perform operation while index is closing.");
            case CLOSED:
                throw new IllegalStateException(
                        "Can't perform operation on closed index.");
            case ERROR:
                throw new IllegalStateException("Index is in ERROR state.",
                        failure);
            default:
                throw new IllegalStateException(
                        "Unsupported SegmentIndex state: " + current);
        }
    }

    private static IllegalStateException markReadyFailure(
            final SegmentIndexState current) {
        switch (current) {
            case READY:
                return new IllegalStateException(
                        "Can't make ready already ready index.");
            case CLOSING:
                return new IllegalStateException(
                        "Can't make ready index while it is closing.");
            case CLOSED:
                return new IllegalStateException(
                        "Can't make ready already closed index.");
            case ERROR:
                return new IllegalStateException(
                        "Can't make ready index in ERROR.");
            default:
                return new IllegalStateException(
                        "Unsupported SegmentIndex state: " + current);
        }
    }

    private static IllegalStateException beginCloseFailure(
            final SegmentIndexState current) {
        switch (current) {
            case OPENING:
                return new IllegalStateException(
                        "Can't close uninitialized index.");
            case CLOSING:
                return new IllegalStateException(
                        "Can't close already closing index.");
            case CLOSED:
                return new IllegalStateException(
                        "Can't close already closed index.");
            default:
                return new IllegalStateException(
                        "Unsupported SegmentIndex state: " + current);
        }
    }

    private static IllegalStateException completeCloseFailure(
            final SegmentIndexState current) {
        switch (current) {
            case OPENING:
                return new IllegalStateException(
                        "Can't finish close while opening.");
            case READY:
                return new IllegalStateException(
                        "Can't finish close from READY state.");
            default:
                return new IllegalStateException(
                        "Unsupported SegmentIndex state: " + current);
        }
    }
}
