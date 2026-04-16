package org.hestiastore.index.segmentindex.split;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class PreparedRouteSplitTest {

    @Test
    void rejectsNullPlan() {
        assertThrows(IllegalArgumentException.class,
                () -> new PreparedRouteSplit<Integer>(null));
    }

    @Test
    void exposesPreparedPlanToSplitPackage() {
        final RouteSplitPlan<Integer> plan = new RouteSplitPlan<>(
                SegmentId.of(1), SegmentId.of(2), SegmentId.of(3), 1, 2,
                RouteSplitPlan.SplitMode.SPLIT);

        final PreparedRouteSplit<Integer> preparedSplit = new PreparedRouteSplit<>(
                plan);

        assertSame(plan, preparedSplit.plan());
    }
}
