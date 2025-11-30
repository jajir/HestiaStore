package org.hestiastore.index.segmentindex;

public interface IndexState<K, V> {

    void onReady(SegmentIndexImpl<K, V> index);

    void onClose(SegmentIndexImpl<K, V> index);

    /**
     * Method check that index is ready for search and manipulation operation.
     * 
     * @throws IllegalStateException when index manipulation is not allowed
     */
    void tryPerformOperation();

}
