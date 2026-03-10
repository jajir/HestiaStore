package org.hestiastore.index.segmentindex.partition;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.split.SegmentSplitApplyPlan;

/**
 * Runtime-only record describing a routed split whose stable child data still
 * need to be materialized from the retired parent segment.
 *
 * @param <K> key type
 */
public final class PendingPartitionSplit<K> {

    private final SegmentSplitApplyPlan<K> applyPlan;

    public PendingPartitionSplit(final SegmentSplitApplyPlan<K> applyPlan) {
        this.applyPlan = Vldtn.requireNonNull(applyPlan, "applyPlan");
    }

    public SegmentSplitApplyPlan<K> getApplyPlan() {
        return applyPlan;
    }

    public SegmentId getParentSegmentId() {
        return applyPlan.getOldSegmentId();
    }

    public SegmentId getLowerSegmentId() {
        return applyPlan.getLowerSegmentId();
    }

    public Optional<SegmentId> getUpperSegmentId() {
        return applyPlan.getUpperSegmentId();
    }

    public K getMaxKey() {
        return applyPlan.getMaxKey();
    }

    public List<SegmentId> getChildSegmentIds() {
        return applyPlan.getUpperSegmentId()
                .map(upper -> List.of(applyPlan.getLowerSegmentId(), upper))
                .orElseGet(() -> List.of(applyPlan.getLowerSegmentId()));
    }
}
