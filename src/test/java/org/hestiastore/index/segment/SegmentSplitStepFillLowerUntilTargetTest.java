package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.PairIteratorList;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.WriteTransaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitStepFillLowerUntilTargetTest {

    @Mock
    private SegmentDeltaCacheController<Integer, String> dc;
    @Mock
    private SegmentPropertiesManager spm;

    private SegmentSplitStepFillLowerUntilTarget<Integer, String> step;

    @BeforeEach
    void setup() {
        step = new SegmentSplitStepFillLowerUntilTarget<>();
    }

    @AfterEach
    void tearDown() {
        step = null;
    }

    private SegmentSplitterPlan<Integer, String> feasiblePlan() {
        when(spm.getSegmentStats()).thenReturn(new SegmentStats(0, 6, 0));
        when(dc.getDeltaCacheSizeWithoutTombstones()).thenReturn(0);
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
                () -> step.perform(
                        new SegmentSplitContext<>(null, null, null, null),
                        null));
        assertEquals("Property 'state' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_plan() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.perform(
                        new SegmentSplitContext<>(null, null, null, null),
                        new SegmentSplitState<>()));
        assertEquals("Property 'plan' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_lowerSegment() {
        final SegmentSplitContext<Integer, String> ctx = new SegmentSplitContext<>(
                null, null, feasiblePlan(), null);
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.perform(ctx, new SegmentSplitState<>()));
        assertEquals("Property 'lowerSegment' must not be null.",
                err.getMessage());
    }

    @Test
    void test_missing_iterator() {
        final SegmentSplitStepFillLowerUntilTarget<Integer, String> step = new SegmentSplitStepFillLowerUntilTarget<>();
        final SegmentSplitContext<Integer, String> ctx = new SegmentSplitContext<>(
                null, null, feasiblePlan(), null);
        final SegmentSplitState<Integer, String> state = new SegmentSplitState<>();
        @SuppressWarnings("unchecked")
        final SegmentImpl<Integer, String> lower = org.mockito.Mockito
                .mock(SegmentImpl.class);
        state.setLowerSegment(lower);
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.perform(ctx, state));
        assertEquals("Property 'iterator' must not be null.", err.getMessage());
    }

    @Test
    void writes_pairs_until_half_and_commits() {
        @SuppressWarnings("unchecked")
        final SegmentImpl<Integer, String> lower = org.mockito.Mockito
                .mock(SegmentImpl.class);
        @SuppressWarnings("unchecked")
        final WriteTransaction<Integer, String> tx = org.mockito.Mockito
                .mock(WriteTransaction.class);
        @SuppressWarnings("unchecked")
        final PairWriter<Integer, String> writer = org.mockito.Mockito
                .mock(PairWriter.class);
        when(lower.openFullWriteTx()).thenReturn(tx);
        when(tx.open()).thenReturn(writer);

        final List<Pair<Integer, String>> data = List.of(Pair.of(1, "a"),
                Pair.of(2, "b"), Pair.of(3, "c"), Pair.of(4, "d"));
        final PairIterator<Integer, String> it = new PairIteratorList<>(data);

        final SegmentSplitState<Integer, String> state = new SegmentSplitState<>();
        state.setLowerSegment(lower);
        state.setIterator(it);
        final SegmentSplitContext<Integer, String> ctx = new SegmentSplitContext<>(
                null, null, feasiblePlan(), null);
        step.perform(ctx, state);
        // half of 6 is 3; expect 3 writes
        verify(writer, times(3)).write(any());
        verify(tx).commit();
    }
}
