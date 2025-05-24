package com.hestiastore.index.segment;

/**
 * This factory is used for creating new instance of {@link SegmentData}.
 * 
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentDataFactory<K, V> {

    /**
     * Create new instance of {@link SegmentData}.
     * 
     * @return new instance of {@link SegmentData}
     */
    SegmentData<K, V> getSegmentData();

}
