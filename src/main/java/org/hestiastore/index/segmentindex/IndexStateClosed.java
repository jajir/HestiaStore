package org.hestiastore.index.segmentindex;

/**
 * Index state representing a closed index.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class IndexStateClosed<K, V> implements IndexState<K, V> {

    /** {@inheritDoc} */
    @Override
    public void onReady(SegmentIndexImpl<K, V> index) {
        throw new IllegalStateException(
                "Can't make ready already closed index.");
    }

    /** {@inheritDoc} */
    @Override
    public void onClose(SegmentIndexImpl<K, V> index) {
        throw new IllegalStateException("Can't close already closed index.");
    }

    /** {@inheritDoc} */
    @Override
    public void tryPerformOperation() {
        throw new IllegalStateException(
                "Can't perform operation on closed index.");
    }
}
