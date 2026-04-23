package org.hestiastore.index.segmentindex.core.session.state;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * Owns state transitions between OPENING, READY, CLOSING, CLOSED, and ERROR.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexStateCoordinator<K, V> {

    private final IndexStateFileLockResolver<K, V> fileLockResolver = new IndexStateFileLockResolver<>();
    private volatile IndexState<K, V> indexState;

    /**
     * Creates a lifecycle coordinator with an explicit initial internal and
     * externally visible state.
     *
     * @param initialState initial internal state object
     * @param initialSegmentState externally exposed lifecycle state
     */
    public IndexStateCoordinator(final IndexState<K, V> initialState,
            final SegmentIndexState initialSegmentState) {
        this.indexState = Vldtn.requireNonNull(initialState, "initialState");
        final SegmentIndexState expectedInitialState = Vldtn
                .requireNonNull(initialSegmentState, "initialSegmentState");
        if (initialState.state() != expectedInitialState) {
            throw new IllegalArgumentException(
                    "Initial index state does not match exposed segment state.");
        }
    }

    /**
     * Starts the close transition.
     */
    public void beginClose() {
        transitionTo(getIndexState().onClose());
    }

    /**
     * Marks startup as complete and transitions to READY.
     */
    public void markReady() {
        transitionTo(getIndexState().onReady());
    }

    /**
     * Installs the ERROR state for the provided failure.
     *
     * @param failure original runtime failure
     */
    public void failWithError(final Throwable failure) {
        installErrorState(failure);
    }

    /**
     * Finalizes a close transition once runtime resources were released.
     * Calling this method on an already closed state is a no-op.
     */
    public void completeCloseStateTransition() {
        final IndexState<K, V> currentState = getIndexState();
        if (currentState instanceof IndexStateClosed<?, ?>) {
            return;
        }
        transitionTo(currentState.finishClose());
    }

    /**
     * @return current internal lifecycle state object
     */
    public IndexState<K, V> getIndexState() {
        return indexState;
    }

    /**
     * @return externally visible lifecycle state
     */
    public SegmentIndexState getState() {
        return getIndexState().state();
    }

    private void transitionTo(final IndexState<K, V> nextState) {
        this.indexState = Vldtn.requireNonNull(nextState, "nextState");
    }

    private void installErrorState(final Throwable failure) {
        transitionTo(new IndexStateError<>(failure,
                fileLockResolver.resolve(getIndexState())));
    }
}
