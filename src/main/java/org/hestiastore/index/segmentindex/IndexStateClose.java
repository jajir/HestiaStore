package org.hestiastore.index.segmentindex;

public class IndexStateClose<K, V> implements IndexState<K, V> {

    @Override
    public void onReady(SegmentIndexImpl<K, V> index) {
        throw new IllegalStateException(
                "Can't make ready already closed index.");
    }

    @Override
    public void onClose(SegmentIndexImpl<K, V> index) {
        throw new IllegalStateException("Can't close already closed index.");
    }

    @Override
    public void tryPerformOperation() {
        throw new IllegalStateException(
                "Can't perform operation on closed index.");
    }
}
