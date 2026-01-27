package org.hestiastore.index.segmentindex;

/**
 * State interface for the index lifecycle state machine.
 *
 * @param <K> key type
 * @param <V> value type
 */
interface IndexState<K, V> {

    /**
     * Handles transition to READY.
     *
     * @param index index instance
     */
    void onReady(SegmentIndexImpl<K, V> index);

    /**
     * Handles transition to CLOSED.
     *
     * @param index index instance
     */
    void onClose(SegmentIndexImpl<K, V> index);

    /**
     * Method check that index is ready for search and manipulation operation.
     * 
     * @throws IllegalStateException when index manipulation is not allowed
     */
    void tryPerformOperation();

}
