package org.hestiastore.index.segmentindex.core.splitplanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class SplitPlannerStateTest {

    @Test
    void awaitNextCycleDrainsHintsAndRescanRequest() {
        final SplitPlannerState plannerState = new SplitPlannerState();
        plannerState.hintSegment(SegmentId.of(1));
        plannerState.hintSegment(SegmentId.of(2));
        plannerState.requestRescan();

        final SplitPlannerState.PlannerCycle cycle = plannerState.awaitNextCycle(
                1L, () -> false);

        assertEquals(List.of(SegmentId.of(1), SegmentId.of(2)),
                cycle.hintedSegments());
        assertTrue(cycle.rescanRequested());
        assertFalse(cycle.forceRetryRequested());
        plannerState.finishCycle();
        assertTrue(plannerState.isSettled());
    }

    @Test
    void clearPendingWorkResetsPlannerSignals() {
        final SplitPlannerState plannerState = new SplitPlannerState();
        plannerState.hintSegment(SegmentId.of(9));
        plannerState.requestRescan();
        plannerState.requestForceRetry();

        plannerState.clearPendingWork();

        assertFalse(plannerState.hasPendingHints());
        assertTrue(plannerState.isSettled());
    }

    @Test
    void requestForceRetryProducesDedicatedCycle() {
        final SplitPlannerState plannerState = new SplitPlannerState();
        plannerState.requestForceRetry();

        final SplitPlannerState.PlannerCycle cycle = plannerState.awaitNextCycle(
                1L, () -> false);

        assertTrue(cycle.forceRetryRequested());
        assertFalse(cycle.rescanRequested());
        assertTrue(cycle.hintedSegments().isEmpty());
    }
}
