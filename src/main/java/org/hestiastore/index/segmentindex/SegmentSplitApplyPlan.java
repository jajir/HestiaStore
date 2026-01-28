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
final class SegmentSplitApplyPlan<K, V> {

    private final SegmentId oldSegmentId;
    private final SegmentId lowerSegmentId;
    private final SegmentId upperSegmentId;
    private final K minKey;
    private final K maxKey;
    private final SegmentSplitterResult.SegmentSplittingStatus status;

    SegmentSplitApplyPlan(final SegmentId oldSegmentId,
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

    SegmentId getOldSegmentId() {
        return oldSegmentId;
    }

    SegmentId getLowerSegmentId() {
        return lowerSegmentId;
    }

    Optional<SegmentId> getUpperSegmentId() {
        return Optional.ofNullable(upperSegmentId);
    }

    K getMinKey() {
        return minKey;
    }

    K getMaxKey() {
        return maxKey;
    }

    SegmentSplitterResult.SegmentSplittingStatus getStatus() {
        return status;
    }
}
