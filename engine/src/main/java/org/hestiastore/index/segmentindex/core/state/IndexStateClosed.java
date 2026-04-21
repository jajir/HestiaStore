package org.hestiastore.index.segmentindex.core.state;

import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * Index state representing a closed index.
 *
 * @param <K> key type
 * @param <V> value type
 */
class IndexStateClosed<K, V> implements IndexState<K, V> {

    /** {@inheritDoc} */
    @Override
    public SegmentIndexState state() {
        return SegmentIndexState.CLOSED;
    }

    /** {@inheritDoc} */
    @Override
    public IndexState<K, V> onReady() {
        throw new IllegalStateException(
                "Can't make ready already closed index.");
    }

    /** {@inheritDoc} */
    @Override
    public IndexState<K, V> onClose() {
        throw new IllegalStateException("Can't close already closed index.");
    }

    /** {@inheritDoc} */
    @Override
    public IndexState<K, V> finishClose() {
        throw new IllegalStateException("Can't finish close already closed.");
    }

    /** {@inheritDoc} */
    @Override
    public void tryPerformOperation() {
        throw new IllegalStateException(
                "Can't perform operation on closed index.");
    }
}
