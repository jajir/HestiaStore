package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.Entry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitStepEnsureLowerNotEmptyTest {

    private SegmentSplitStepEnsureLowerNotEmpty<Integer, String> step;

    @BeforeEach
    void setup() {
        step = new SegmentSplitStepEnsureLowerNotEmpty<>();
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
    void test_missing_plan() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.filter(
                        new SegmentSplitContext<>(null, null, null, null, null),
                        new SegmentSplitState<>()));
        assertEquals("Property 'plan' must not be null.", err.getMessage());
    }

    @Test
    void throws_when_lower_empty_and_passes_when_not() {
        final SegmentSplitContext<Integer, String> ctxEmpty = new SegmentSplitContext<>(
                null, planWithEstimate(10), null, null, null);
        assertThrows(IllegalStateException.class,
                () -> step.filter(ctxEmpty, new SegmentSplitState<>()));

        final SegmentSplitterPlan<Integer, String> plan = planWithEstimate(10);
        plan.recordLower(Entry.of(1, "a"));
        final SegmentSplitContext<Integer, String> ctxNonEmpty = new SegmentSplitContext<>(
                null, plan, null, null, null);
        assertDoesNotThrow(
                () -> step.filter(ctxNonEmpty, new SegmentSplitState<>()));
    }
}
