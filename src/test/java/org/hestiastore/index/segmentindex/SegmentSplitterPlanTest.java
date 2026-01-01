package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.Entry;
import org.junit.jupiter.api.Test;

class SegmentSplitterPlanTest {

    private SegmentSplitterPlan<String, String> newPlan() {
        final SegmentSplitterPolicy<String, String> policy = new SegmentSplitterPolicy<>(
                3L, false);
        return SegmentSplitterPlan.fromPolicy(policy);
    }

    @Test
    void isLowerSegmentEmpty_initially_true() {
        final SegmentSplitterPlan<String, String> plan = newPlan();
        assertTrue(plan.isLowerSegmentEmpty());
    }

    @Test
    void isLowerSegmentEmpty_after_recordLower_false() {
        final SegmentSplitterPlan<String, String> plan = newPlan();
        plan.recordLower(Entry.of("k1", "v1"));
        assertFalse(plan.isLowerSegmentEmpty());
    }
}
