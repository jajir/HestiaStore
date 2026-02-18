package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class SegmentSplitCoordinatorApplyPlanTest {

    @Test
    void toApplyPlan_includesUpperSegmentWhenSplit() {
        final SegmentId oldSegmentId = SegmentId.of(1);
        final SegmentId lowerSegmentId = SegmentId.of(2);
        final SegmentId upperSegmentId = SegmentId.of(3);
        final SegmentSplitterResult<Integer, String> result = new SegmentSplitterResult<>(
                lowerSegmentId, 1, 10,
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);

        final SegmentSplitApplyPlan<Integer, String> plan = SegmentSplitCoordinator
                .toApplyPlan(oldSegmentId, upperSegmentId, result);

        assertEquals(oldSegmentId, plan.getOldSegmentId());
        assertEquals(lowerSegmentId, plan.getLowerSegmentId());
        assertTrue(plan.getUpperSegmentId().isPresent());
        assertEquals(upperSegmentId, plan.getUpperSegmentId().orElseThrow());
        assertEquals(1, plan.getMinKey());
        assertEquals(10, plan.getMaxKey());
        assertEquals(SegmentSplitterResult.SegmentSplittingStatus.SPLIT,
                plan.getStatus());
    }

    @Test
    void toApplyPlan_omitsUpperSegmentWhenCompacted() {
        final SegmentId oldSegmentId = SegmentId.of(1);
        final SegmentId lowerSegmentId = SegmentId.of(2);
        final SegmentId upperSegmentId = SegmentId.of(3);
        final SegmentSplitterResult<Integer, String> result = new SegmentSplitterResult<>(
                lowerSegmentId, 1, 10,
                SegmentSplitterResult.SegmentSplittingStatus.COMPACTED);

        final SegmentSplitApplyPlan<Integer, String> plan = SegmentSplitCoordinator
                .toApplyPlan(oldSegmentId, upperSegmentId, result);

        assertEquals(oldSegmentId, plan.getOldSegmentId());
        assertEquals(lowerSegmentId, plan.getLowerSegmentId());
        assertTrue(plan.getUpperSegmentId().isEmpty());
        assertEquals(1, plan.getMinKey());
        assertEquals(10, plan.getMaxKey());
        assertEquals(SegmentSplitterResult.SegmentSplittingStatus.COMPACTED,
                plan.getStatus());
    }
}
