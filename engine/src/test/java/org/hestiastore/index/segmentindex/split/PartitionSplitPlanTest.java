package org.hestiastore.index.segmentindex.split;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.Entry;
import org.junit.jupiter.api.Test;

class PartitionSplitPlanTest {

    private PartitionSplitPlan<String, String> newPlan() {
        final PartitionSplitPolicy policy = new PartitionSplitPolicy(3L);
        return PartitionSplitPlan.fromPolicy(policy);
    }

    @Test
    void isLowerSegmentEmpty_initially_true() {
        final PartitionSplitPlan<String, String> plan = newPlan();
        assertTrue(plan.isLowerSegmentEmpty());
    }

    @Test
    void isLowerSegmentEmpty_after_recordLower_false() {
        final PartitionSplitPlan<String, String> plan = newPlan();
        plan.recordLower(Entry.of("k1", "v1"));
        assertFalse(plan.isLowerSegmentEmpty());
    }
}
