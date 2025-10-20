package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitStepCreateLowerSegmentTest {

    private SegmentSplitStepCreateLowerSegment<Integer, String> step;
    @Mock
    private SegmentImpl<Integer, String> segment;
    @Mock
    private SegmentImpl<Integer, String> created;

    @BeforeEach
    void setup() {
        step = new SegmentSplitStepCreateLowerSegment<>();
    }

    @AfterEach
    void tearDown() {
        step = null;
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
    void test_missing_segment() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.perform(new SegmentSplitContext<>(null, null, null,
                        SegmentId.of(1)), new SegmentSplitState<>()));
        assertEquals("Property 'segment' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_lowerSegmentId() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.perform(
                        new SegmentSplitContext<>(segment, null, null, null),
                        new SegmentSplitState<>()));
        assertEquals("Property 'lowerSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void calls_createSegmentWithSameConfig() {
        final SegmentId id = SegmentId.of(1);
        when(segment.createSegmentWithSameConfig(id)).thenReturn(created);

        final SegmentSplitContext<Integer, String> ctx = new SegmentSplitContext<>(
                segment, null, null, id);
        final SegmentSplitState<Integer, String> state = new SegmentSplitState<>();
        step.perform(ctx, state);
        verify(segment).createSegmentWithSameConfig(id);
        assertNotNull(state.getLowerSegment());
    }
}
