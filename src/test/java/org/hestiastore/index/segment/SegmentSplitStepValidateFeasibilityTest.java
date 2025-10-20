package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitStepValidateFeasibilityTest {

    @Mock
    private SegmentDeltaCacheController<Integer, String> dc;
    @Mock
    private SegmentPropertiesManager spm;

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
        when(spm.getSegmentStats())
                .thenReturn(new SegmentStats(0, estimate, 0));
        when(dc.getDeltaCacheSizeWithoutTombstones())
                .thenReturn(0);
        final SegmentSplitterPolicy<Integer, String> policy = new SegmentSplitterPolicy<>(
                spm, dc);
        return SegmentSplitterPlan.fromPolicy(policy);
    }

    @Test
    void test_missing_ctx() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.perform(null, new SegmentSplitState<>()));
        assertEquals("Property 'ctx' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_state() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.perform(new SegmentSplitContext<>(null, null, null, null), null));
        assertEquals("Property 'state' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_plan() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.perform(new SegmentSplitContext<>(null, null, null, null), new SegmentSplitState<>()));
        assertEquals("Property 'plan' must not be null.", err.getMessage());
    }

    @Test
    void throws_when_not_feasible_and_passes_when_feasible() {
        final SegmentSplitContext<Integer, String> ctxNot = new SegmentSplitContext<>(
                null, null, planWithEstimate(2), null);
        assertThrows(IllegalStateException.class,
                () -> step.perform(ctxNot, new SegmentSplitState<>()));

        final SegmentSplitContext<Integer, String> ctxOk = new SegmentSplitContext<>(
                null, null, planWithEstimate(10), null);
        assertDoesNotThrow(
                () -> step.perform(ctxOk, new SegmentSplitState<>()));
    }
}
