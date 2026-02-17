package org.hestiastore.index.segmentindex;

import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

/**
 * Immutable DTO carrying data required to apply a completed split.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentSplitApplyPlan<K, V> {

    private final SegmentId oldSegmentId;
    private final SegmentId lowerSegmentId;
    private final SegmentId upperSegmentId;
    private final K minKey;
    private final K maxKey;
    private final SegmentSplitterResult.SegmentSplittingStatus status;

    /**
     * Creates an immutable plan describing how to apply split/compaction
     * outcome to the key-to-segment map.
     *
     * @param oldSegmentId replaced segment id
     * @param lowerSegmentId newly created lower segment id
     * @param upperSegmentId new upper segment id; required for SPLIT status
     * @param minKey minimum key covered by the lower segment
     * @param maxKey maximum key covered by the lower segment
     * @param status split outcome status
     */
    public SegmentSplitApplyPlan(final SegmentId oldSegmentId,
            final SegmentId lowerSegmentId,
            final SegmentId upperSegmentId, final K minKey, final K maxKey,
            final SegmentSplitterResult.SegmentSplittingStatus status) {
        this.oldSegmentId = Vldtn.requireNonNull(oldSegmentId, "oldSegmentId");
        this.lowerSegmentId = Vldtn.requireNonNull(lowerSegmentId,
                "lowerSegmentId");
        this.status = Vldtn.requireNonNull(status, "status");
        if (status == SegmentSplitterResult.SegmentSplittingStatus.SPLIT) {
            this.upperSegmentId = Vldtn.requireNonNull(upperSegmentId,
                    "upperSegmentId");
        } else {
            this.upperSegmentId = upperSegmentId;
        }
        this.minKey = Vldtn.requireNonNull(minKey, "minKey");
        this.maxKey = Vldtn.requireNonNull(maxKey, "maxKey");
    }

    /**
     * @return id of the segment being replaced
     */
    public SegmentId getOldSegmentId() {
        return oldSegmentId;
    }

    /**
     * @return id of the lower segment produced by split/compaction
     */
    public SegmentId getLowerSegmentId() {
        return lowerSegmentId;
    }

    /**
     * @return optional upper segment id (present for SPLIT, absent for COMPACTED)
     */
    public Optional<SegmentId> getUpperSegmentId() {
        return Optional.ofNullable(upperSegmentId);
    }

    /**
     * @return minimum key covered by the lower segment
     */
    public K getMinKey() {
        return minKey;
    }

    /**
     * @return maximum key covered by the lower segment
     */
    public K getMaxKey() {
        return maxKey;
    }

    /**
     * @return split outcome status
     */
    public SegmentSplitterResult.SegmentSplittingStatus getStatus() {
        return status;
    }
}
