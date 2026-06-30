package org.hestiastore.index.segmentindex.core;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * Owns state transitions between OPENING, READY, CLOSING, CLOSED, and ERROR.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class IndexStateCoordinator<K, V> {

    private volatile IndexState<K, V> indexState;
    private volatile SegmentIndexState segmentIndexState;

    IndexStateCoordinator(final IndexState<K, V> initialState,
            final SegmentIndexState initialSegmentState) {
        this.indexState = Vldtn.requireNonNull(initialState, "initialState");
        this.segmentIndexState = Vldtn.requireNonNull(initialSegmentState,
                "initialSegmentState");
    }

    void beginClose(final SegmentIndexImpl<K, V> index) {
        getIndexState().onClose(index);
        setSegmentIndexState(SegmentIndexState.CLOSING);
    }

    void markReady(final SegmentIndexImpl<K, V> index) {
        getIndexState().onReady(index);
        setSegmentIndexState(SegmentIndexState.READY);
    }

    void failWithError(final Throwable failure) {
        setSegmentIndexState(SegmentIndexState.ERROR);
        setIndexState(new IndexStateError<>(failure,
                resolveFileLock(getIndexState())));
    }

    void completeCloseStateTransition(final SegmentIndexImpl<K, V> index) {
        if (getState() != SegmentIndexState.ERROR) {
            setSegmentIndexState(SegmentIndexState.CLOSED);
        }
        final IndexState<K, V> currentState = getIndexState();
        if (currentState instanceof IndexStateClosing<?, ?>) {
            @SuppressWarnings("unchecked")
            final IndexStateClosing<K, V> closingState = (IndexStateClosing<K, V>) currentState;
            closingState.finishClose(index);
            return;
        }
        if (currentState instanceof IndexStateClosed<?, ?>) {
            return;
        }
        currentState.onClose(index);
    }

    IndexState<K, V> getIndexState() {
        return indexState;
    }

    SegmentIndexState getState() {
        return segmentIndexState;
    }

    void setIndexState(final IndexState<K, V> indexState) {
        this.indexState = Vldtn.requireNonNull(indexState, "indexState");
    }

    void setSegmentIndexState(final SegmentIndexState state) {
        this.segmentIndexState = Vldtn.requireNonNull(state, "state");
    }

    private FileLock resolveFileLock(final IndexState<K, V> currentState) {
        if (currentState instanceof IndexStateReady<?, ?> readyState) {
            return readyState.getFileLock();
        }
        if (currentState instanceof IndexStateOpening<?, ?> openingState) {
            return openingState.getFileLock();
        }
        if (currentState instanceof IndexStateClosing<?, ?> closingState) {
            return closingState.getFileLock();
        }
        return null;
    }
}
