package org.hestiastore.index.segmentindex.core.state;

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

    public void beginClose() {
        transitionTo(getIndexState().onClose());
    }

    public void markReady() {
        transitionTo(getIndexState().onReady());
    }

    public void failWithError(final Throwable failure) {
        installErrorState(failure);
    }

    public void completeCloseStateTransition() {
        final IndexState<K, V> currentState = getIndexState();
        if (currentState instanceof IndexStateClosed<?, ?>) {
            return;
        }
        transitionTo(currentState.finishClose());
    }

    public IndexState<K, V> getIndexState() {
        return indexState;
    }

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
