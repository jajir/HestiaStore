package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.hestiastore.index.segment.SegmentId;

@ExtendWith(MockitoExtension.class)
class SegmentSplitStepReplaceIfNoRemainingTest {

    private SegmentSplitStepReplaceIfNoRemaining<Integer, String> step;

    @BeforeEach
    void setup() {
        step = new SegmentSplitStepReplaceIfNoRemaining<>();
    }

    @AfterEach
    void tearDown() {
        step = null;
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
                () -> step.filter(
                        new SegmentSplitContext<>(null, null, null, null),
                        null));
        assertEquals("Property 'state' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_iterator() {
        final SegmentSplitContext<Integer, String> ctx = new SegmentSplitContext<>(
                null, SegmentSplitterPlan.fromPolicy(
                        new SegmentSplitterPolicy<>(5, false)),
                SegmentId.of(1), null);
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.filter(ctx, new SegmentSplitState<>()));
        assertEquals("Property 'iterator' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_lower_segment_id() {
        final SegmentSplitContext<Integer, String> ctx = new SegmentSplitContext<>(
                null, SegmentSplitterPlan.fromPolicy(
                        new SegmentSplitterPolicy<>(5, false)),
                null, null);
        final SegmentSplitState<Integer, String> state = new SegmentSplitState<>();
        state.setIterator(
                new EntryIteratorList<Integer, String>(java.util.List.of()));
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.filter(ctx, state));
        assertEquals("Property 'lowerSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void sets_result_when_no_remaining_and_skips_when_remaining() {
        final SegmentSplitterPlan<Integer, String> plan = SegmentSplitterPlan
                .fromPolicy(new SegmentSplitterPolicy<>(5, false));
        plan.recordLower(Entry.of(1, "a"));

        final SegmentSplitState<Integer, String> state1 = new SegmentSplitState<>();
        state1.setLowerSegmentId(SegmentId.of(10));
        state1.setIterator(new EntryIteratorList<Integer, String>(
                List.of(Entry.of(1, "a"))));
        final SegmentSplitContext<Integer, String> ctx = new SegmentSplitContext<>(
                null, plan, SegmentId.of(10), null);
        assertEquals(true, step.filter(ctx, state1));
        assertNull(state1.getResult());

        final SegmentSplitState<Integer, String> state2 = new SegmentSplitState<>();
        state2.setLowerSegmentId(SegmentId.of(10));
        state2.setIterator(new EntryIteratorList<Integer, String>(List.of()));
        step.filter(ctx, state2);
        assertEquals(SegmentSplitterResult.SegmentSplittingStatus.COMPACTED,
                state2.getResult().getStatus());
    }
}
