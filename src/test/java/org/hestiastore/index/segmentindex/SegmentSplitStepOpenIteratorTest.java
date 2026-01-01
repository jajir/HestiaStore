package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.EntryIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.hestiastore.index.segment.Segment;

@ExtendWith(MockitoExtension.class)
class SegmentSplitStepOpenIteratorTest {

    private SegmentSplitStepOpenIterator<Integer, String> step;
    @Mock
    private Segment<Integer, String> segment;
    @Mock
    private EntryIterator<Integer, String> it;

    @BeforeEach
    void setup() {
        step = new SegmentSplitStepOpenIterator<>();
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
        when(segment.openIterator()).thenReturn(it);
        final SegmentSplitContext<Integer, String> ctx = new SegmentSplitContext<>(
                segment, null, null, null, null);
        step.filter(ctx, new SegmentSplitState<>());
        verify(segment).openIterator();
    }
}
