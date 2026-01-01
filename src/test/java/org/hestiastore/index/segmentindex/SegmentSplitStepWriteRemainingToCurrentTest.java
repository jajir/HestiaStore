package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.WriteTransaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;

@ExtendWith(MockitoExtension.class)
class SegmentSplitStepWriteRemainingToCurrentTest {

    @Mock
    private Segment<Integer, String> segment;
    @Mock
    private WriteTransaction<Integer, String> tx;
    @Mock
    private EntryWriter<Integer, String> writer;

    private SegmentSplitStepWriteRemainingToCurrent<Integer, String> step;

    @BeforeEach
    void setup() {
        step = new SegmentSplitStepWriteRemainingToCurrent<>();
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
                () -> step.filter(
                        new SegmentSplitContext<>(null, null, null, null, null),
                        null));
        assertEquals("Property 'state' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_segment() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.filter(new SegmentSplitContext<>(null,
                        SegmentSplitterPlan.fromPolicy(
                                new SegmentSplitterPolicy<>(5, false)),
                        SegmentId.of(1), SegmentId.of(2), id -> tx),
                        new SegmentSplitState<>()));
        assertEquals("Property 'segment' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_plan() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.filter(
                        new SegmentSplitContext<>(segment, null, null, null,
                                null),
                        new SegmentSplitState<>()));
        assertEquals("Property 'plan' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_iterator() {
        final SegmentSplitterPlan<Integer, String> plan = SegmentSplitterPlan
                .fromPolicy(new SegmentSplitterPolicy<>(5, false));
        plan.recordLower(Entry.of(0, "z"));
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.filter(
                        new SegmentSplitContext<>(segment, plan,
                                SegmentId.of(1), SegmentId.of(2), id -> tx),
                        new SegmentSplitState<>()));
        assertEquals("Property 'iterator' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_lower_segment_id() {
        final SegmentSplitterPlan<Integer, String> plan = SegmentSplitterPlan
                .fromPolicy(new SegmentSplitterPolicy<>(5, false));
        plan.recordLower(Entry.of(0, "z"));
        final SegmentSplitState<Integer, String> state = new SegmentSplitState<>();
        state.setIterator(new EntryIteratorList<Integer, String>(
                java.util.List.of(Entry.of(1, "a"))));
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.filter(
                        new SegmentSplitContext<>(segment, plan, null,
                                SegmentId.of(2),
                                id -> tx),
                        state));
        assertEquals("Property 'lowerSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void writes_remaining_and_commits_and_returns_split() {
        when(tx.open()).thenReturn(writer);
        final var it = new EntryIteratorList<Integer, String>(
                List.of(Entry.of(1, "a"), Entry.of(2, "b")));

        final SegmentSplitState<Integer, String> state = new SegmentSplitState<>();
        state.setIterator(it);
        state.setLowerSegmentId(SegmentId.of(2));
        final SegmentSplitterPlan<Integer, String> plan = planWithEstimate(10);
        plan.recordLower(Entry.of(0, "z"));
        final SegmentSplitContext<Integer, String> ctx = new SegmentSplitContext<>(
                segment, plan, SegmentId.of(2), SegmentId.of(99), id -> tx);
        step.filter(ctx, state);
        verify(writer, times(2)).write(any());
        verify(tx).commit();
        assertEquals(SegmentSplitterResult.SegmentSplittingStatus.SPLIT,
                state.getResult().getStatus());
    }
}
