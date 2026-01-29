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

    public SegmentId getOldSegmentId() {
        return oldSegmentId;
    }

    public SegmentId getLowerSegmentId() {
        return lowerSegmentId;
    }

    public Optional<SegmentId> getUpperSegmentId() {
        return Optional.ofNullable(upperSegmentId);
    }

    public K getMinKey() {
        return minKey;
    }

    public K getMaxKey() {
        return maxKey;
    }

    public SegmentSplitterResult.SegmentSplittingStatus getStatus() {
        return status;
    }
}
