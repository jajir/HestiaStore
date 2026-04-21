package org.hestiastore.index.segmentindex.core.state;

import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * State interface for the index lifecycle state machine.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface IndexState<K, V> {

    /**
     * Returns the externally visible lifecycle state represented by this
     * internal state object.
     *
     * @return exposed lifecycle state
     */
    SegmentIndexState state();

    /**
     * Handles transition to READY.
     *
     * @return next lifecycle state
     */
    IndexState<K, V> onReady();

    /**
     * Handles transition to CLOSING.
     *
     * @return next lifecycle state
     */
    IndexState<K, V> onClose();

    /**
     * Completes the close transition once runtime resources were released.
     *
     * @return final lifecycle state after shutdown
     */
    IndexState<K, V> finishClose();

    /**
     * Method check that index is ready for search and manipulation operation.
     * 
     * @throws IllegalStateException when index manipulation is not allowed
     */
    void tryPerformOperation();

}
