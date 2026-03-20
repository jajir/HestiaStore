
package org.hestiastore.index.segmentindex.split;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

/**
 * Provides result metadata for partition split materialization.
 */
public final class PartitionSplitResult<K> {

    /**
     * Status of the parent route after applying split materialization.
     */
    public enum PartitionSplitStatus {
        /**
         * Split produced a replacement using the lower segment; the temporary
         * upper segment id was not used.
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
    private final PartitionSplitStatus status;

    /**
     * Creates a split result with the provided segment metadata.
     *
     * @param segmentId segment id for the lower segment
     * @param minKey minimum key of the lower segment
     * @param maxKey maximum key of the lower segment
     * @param partitionSplitStatus split status
     */
    public PartitionSplitResult(final SegmentId segmentId, final K minKey,
            final K maxKey, final PartitionSplitStatus partitionSplitStatus) {
        this.segmentId = Vldtn.requireNonNull(segmentId, "segmentId");
        this.minKey = Vldtn.requireNonNull(minKey, "minKey");
        this.maxKey = Vldtn.requireNonNull(maxKey, "maxKey");
        this.status = Vldtn.requireNonNull(partitionSplitStatus,
                "partitionSplitStatus");
    }

    /**
     * Checks if the segment was split into two segments.
     *
     * @return {@code true} if the segment was split; {@code false} otherwise
     */
    public boolean isSplit() {
        return status == PartitionSplitStatus.SPLIT;
    }

    /**
     * Returns the identifier of the lower segment created during the split.
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
    public PartitionSplitStatus getStatus() {
        return status;
    }
}
