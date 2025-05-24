
package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.Vldtn;

/**
 * Provides result of segment splitting.
 */
public class SegmentSplitterResult<K, V> {

    /**
     * Status of segment after splitting.
     */
    public static enum SegmentSplittingStatus {
        /**
         * Segment was just compacted. It means that segmentId was not used.
         */
        COMPACTED,
        /**
         * Segment was split into two segments. Given segmentId is used.
         */
        SPLITED
    }

    private final Segment<K, V> segment;
    private final K maxKey;
    private final K minKey;
    private final SegmentSplittingStatus status;

    public SegmentSplitterResult(final Segment<K, V> segment, final K minKey,
            final K maxKey,
            final SegmentSplittingStatus segmentSplittingStatus) {
        this.segment = Objects.requireNonNull(segment);
        this.minKey = Objects.requireNonNull(minKey);
        this.maxKey = Objects.requireNonNull(maxKey);
        this.status = Vldtn.requireNonNull(segmentSplittingStatus,
                "segmentSplittingStatus");
    }

    public boolean isSplited() {
        return status == SegmentSplittingStatus.SPLITED;
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