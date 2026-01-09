package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitStepOpenIteratorTest {

    @Mock
    private Segment<Integer, String> segment;
    @Mock
    private EntryIterator<Integer, String> iterator;

    private SegmentSplitStepOpenIterator<Integer, String> step;
    private SegmentSplitContext<Integer, String> context;
    private SegmentSplitState<Integer, String> state;

    @BeforeEach
    void setUp() {
        step = new SegmentSplitStepOpenIterator<>();
        final SegmentSplitterPlan<Integer, String> plan = SegmentSplitterPlan
                .fromPolicy(new SegmentSplitterPolicy<>(4, false));
        context = new SegmentSplitContext<>(segment, plan, SegmentId.of(1),
                SegmentId.of(2), id -> null);
        state = new SegmentSplitState<>();
    }

    @AfterEach
    void tearDown() {
        step = null;
        context = null;
        state = null;
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
                () -> step.filter(
                        new SegmentSplitContext<>(null, null, null, null, null),
                        new SegmentSplitState<>()));
        assertEquals("Property 'segment' must not be null.", err.getMessage());
    }

    @Test
    void calls_openIterator_on_segment() {
        when(segment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(SegmentResult.ok(iterator));

        step.filter(context, new SegmentSplitState<>());

        verify(segment).openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
    }

    @Test
    void filter_retries_on_busy_until_ok() {
        when(segment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(SegmentResult.busy())
                .thenReturn(SegmentResult.ok(iterator));

        assertTrue(step.filter(context, state));

        assertSame(iterator, state.getIterator());
        verify(segment, times(2))
                .openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
    }
}
