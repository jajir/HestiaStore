
package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

/**
 * Provides result of segment splitting.
 */
public class SegmentSplitterResult<K, V> {

    /**
     * Status of segment after splitting.
     */
    public enum SegmentSplittingStatus {
        /**
         * Segment was just compacted. It means that segmentId was not used.
         */
        COMPACTED,
        /**
         * Segment was split into two segments. Given segmentId is used.
         */
        SPLIT
    }

    private final Segment<K, V> segment;
    private final K maxKey;
    private final K minKey;
    private final SegmentSplittingStatus status;

    public SegmentSplitterResult(final Segment<K, V> segment, final K minKey,
            final K maxKey,
            final SegmentSplittingStatus segmentSplittingStatus) {
        this.segment = Vldtn.requireNonNull(segment, "segment");
        this.minKey = Vldtn.requireNonNull(minKey, "minKey");
        this.maxKey = Vldtn.requireNonNull(maxKey, "maxKey");
        this.status = Vldtn.requireNonNull(segmentSplittingStatus,
                "segmentSplittingStatus");
    }

    /**
     * Checks if the segment was split into two segments.
     *
     * @return {@code true} if the segment was split; {@code false} otherwise
     */
    public boolean isSplit() {
        return status == SegmentSplittingStatus.SPLIT;
    }

    /**
     * Returns the segment resulting from the split or compaction.
     *
     * @return the resulting {@link Segment}
     */
    public Segment<K, V> getSegment() {
        return segment;
    }

    /**
     * Returns the maximum key in the segment after splitting.
     *
     * @return the maximum key
     */
    public K getMaxKey() {
        return maxKey;
    }

    /**
     * Returns the minimum key in the segment after splitting.
     *
     * @return the minimum key
     */
    public K getMinKey() {
        return minKey;
    }

    /**
     * Returns the status of the segment after splitting.
     *
     * @return the segment splitting status
     */
    public SegmentSplittingStatus getStatus() {
        return status;
    }
}
