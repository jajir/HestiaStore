package org.hestiastore.index.segmentindex.split;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitStepValidateFeasibilityTest {

    private SegmentSplitStepValidateFeasibility<Integer, String> step;

    @BeforeEach
    void setup() {
        step = new SegmentSplitStepValidateFeasibility<>();
    }

    @AfterEach
    void tearDown() {
        step = null;
    }

    private SegmentSplitterPlan<Integer, String> planWithEstimate(
            long estimate) {
        final SegmentSplitterPolicy policy = new SegmentSplitterPolicy(
                estimate);
        return SegmentSplitterPlan.fromPolicy(policy);
    }

    @Test
    void test_missing_ctx() {
        final SegmentSplitState<Integer, String> state = new SegmentSplitState<>();
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.filter(null, state));
        assertEquals("Property 'ctx' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_state() {
        final SegmentSplitContext<Integer, String> ctx = new SegmentSplitContext<>(
                null, null, null, null, null);
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.filter(ctx, null));
        assertEquals("Property 'state' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_plan() {
        final SegmentSplitContext<Integer, String> ctx = new SegmentSplitContext<>(
                null, null, null, null, null);
        final SegmentSplitState<Integer, String> state = new SegmentSplitState<>();
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.filter(ctx, state));
        assertEquals("Property 'plan' must not be null.", err.getMessage());
    }

    @Test
    void throws_when_not_feasible_and_passes_when_feasible() {
        final SegmentSplitContext<Integer, String> ctxNot = new SegmentSplitContext<>(
                null, planWithEstimate(2), null, null, null);
        final SegmentSplitState<Integer, String> missingState = new SegmentSplitState<>();
        assertThrows(IllegalStateException.class,
                () -> step.filter(ctxNot, missingState));

        final SegmentSplitContext<Integer, String> ctxOk = new SegmentSplitContext<>(
                null, planWithEstimate(10), null, null, null);
        final SegmentSplitState<Integer, String> validState = new SegmentSplitState<>();
        assertDoesNotThrow(
                () -> step.filter(ctxOk, validState));
    }
}
