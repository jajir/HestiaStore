package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
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
        final SegmentSplitterPolicy<Integer, String> policy = new SegmentSplitterPolicy<>(
                estimate, false);
        return SegmentSplitterPlan.fromPolicy(policy);
    }

    @Test
    void test_missing_ctx() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.filter(null, new SegmentSplitState<>()));
        assertEquals("Property 'ctx' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_state() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.filter(new SegmentSplitContext<>(null, null, null, null, null), null));
        assertEquals("Property 'state' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_plan() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.filter(new SegmentSplitContext<>(null, null, null, null, null), new SegmentSplitState<>()));
        assertEquals("Property 'plan' must not be null.", err.getMessage());
    }

    @Test
    void throws_when_not_feasible_and_passes_when_feasible() {
        final SegmentSplitContext<Integer, String> ctxNot = new SegmentSplitContext<>(
                null, planWithEstimate(2), null, null, null);
        assertThrows(IllegalStateException.class,
                () -> step.filter(ctxNot, new SegmentSplitState<>()));

        final SegmentSplitContext<Integer, String> ctxOk = new SegmentSplitContext<>(
                null, planWithEstimate(10), null, null, null);
        assertDoesNotThrow(
                () -> step.filter(ctxOk, new SegmentSplitState<>()));
    }
}
