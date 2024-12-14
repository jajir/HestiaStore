
package com.coroptis.index.segment;

import java.util.Objects;

/**
 * Provides result of segment splitting.
 */
public class SegmentSplitterResult<K, V> {

    private final Segment<K, V> segment;
    private final K maxKey;
    private final K minKey;

    public SegmentSplitterResult(final Segment<K, V> segment, final K minKey, final K maxKey) {
        this.segment = Objects.requireNonNull(segment);
        this.minKey = Objects.requireNonNull(minKey);
        this.maxKey = Objects.requireNonNull(maxKey);
    }

    public Segment<K, V> getSegment() {
        return segment;
    }

    public K getMaxKey() {
        return maxKey;
    }

    public K getMinKey() {
        return minKey;
    }
}