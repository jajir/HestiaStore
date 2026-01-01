package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.WriteTransaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.hestiastore.index.segment.SegmentId;

@ExtendWith(MockitoExtension.class)
class SegmentSplitStepFillLowerUntilTargetTest {

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
        final SegmentSplitterPolicy<Integer, String> policy = new SegmentSplitterPolicy<>(
                6, false);
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
    void test_missing_plan() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.filter(
                        new SegmentSplitContext<>(null, null, null, null, null),
                        new SegmentSplitState<>()));
        assertEquals("Property 'plan' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_writerTxFactory() {
        final SegmentSplitContext<Integer, String> ctx = new SegmentSplitContext<>(
                null, feasiblePlan(), SegmentId.of(1), null, null);
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.filter(ctx, new SegmentSplitState<>()));
        assertEquals("Property 'writerTxFactory' must not be null.",
                err.getMessage());
    }

    @Test
    void test_missing_iterator() {
        final SegmentSplitStepFillLowerUntilTarget<Integer, String> step = new SegmentSplitStepFillLowerUntilTarget<>();
        final SegmentSplitContext<Integer, String> ctx = new SegmentSplitContext<>(
                null, feasiblePlan(), SegmentId.of(1), null, id -> null);
        final SegmentSplitState<Integer, String> state = new SegmentSplitState<>();
        state.setLowerSegmentId(SegmentId.of(1));
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.filter(ctx, state));
        assertEquals("Property 'iterator' must not be null.", err.getMessage());
    }

    @Test
    void writes_entries_until_half_and_commits() {
        @SuppressWarnings("unchecked")
        final WriteTransaction<Integer, String> tx = org.mockito.Mockito
                .mock(WriteTransaction.class);
        @SuppressWarnings("unchecked")
        final EntryWriter<Integer, String> writer = org.mockito.Mockito
                .mock(EntryWriter.class);
        when(tx.open()).thenReturn(writer);

        final List<Entry<Integer, String>> data = List.of(Entry.of(1, "a"),
                Entry.of(2, "b"), Entry.of(3, "c"), Entry.of(4, "d"));
        final EntryIterator<Integer, String> it = new EntryIteratorList<>(data);

        final SegmentSplitState<Integer, String> state = new SegmentSplitState<>();
        state.setLowerSegmentId(SegmentId.of(1));
        state.setIterator(it);
        final SegmentSplitContext<Integer, String> ctx = new SegmentSplitContext<>(
                null, feasiblePlan(), SegmentId.of(1), null, id -> tx);
        step.filter(ctx, state);
        // half of 6 is 3; expect 3 writes
        verify(writer, times(3)).write(any());
        verify(tx).commit();
    }
}
