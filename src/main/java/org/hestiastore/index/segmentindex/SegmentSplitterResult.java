
package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

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

    private final SegmentId segmentId;
    private final K maxKey;
    private final K minKey;
    private final SegmentSplittingStatus status;

    public SegmentSplitterResult(final SegmentId segmentId, final K minKey,
            final K maxKey,
            final SegmentSplittingStatus segmentSplittingStatus) {
        this.segmentId = Vldtn.requireNonNull(segmentId, "segmentId");
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
     * Returns the identifier of the lower segment created during the split or
     * compaction.
     *
     * @return segment id of the lower segment
     */
    public SegmentId getSegmentId() {
        return segmentId;
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
