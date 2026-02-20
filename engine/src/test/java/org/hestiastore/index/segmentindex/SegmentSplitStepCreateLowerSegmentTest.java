package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitStepCreateLowerSegmentTest {

    private SegmentSplitStepCreateLowerSegment<Integer, String> step;
    @Mock
    private Segment<Integer, String> segment;

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
                () -> step.filter(new SegmentSplitContext<>(null, null,
                        SegmentId.of(1), SegmentId.of(2), id -> null),
                        new SegmentSplitState<>()));
        assertEquals("Property 'segment' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_lowerSegmentId() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.filter(
                        new SegmentSplitContext<>(segment, null, null,
                                SegmentId.of(2),
                                id -> null),
                        new SegmentSplitState<>()));
        assertEquals("Property 'lowerSegmentId' must not be null.",
                err.getMessage());
    }

    @Test
    void test_missing_writerTxFactory() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.filter(new SegmentSplitContext<>(segment, null,
                        SegmentId.of(1), SegmentId.of(2), null),
                        new SegmentSplitState<>()));
        assertEquals("Property 'writerTxFactory' must not be null.",
                err.getMessage());
    }

    @Test
    void stores_lower_segment_id() {
        final SegmentId id = SegmentId.of(1);

        final SegmentSplitContext<Integer, String> ctx = new SegmentSplitContext<>(
                segment, null, id, SegmentId.of(2), ignored -> null);
        final SegmentSplitState<Integer, String> state = new SegmentSplitState<>();
        step.filter(ctx, state);
        assertNotNull(state.getLowerSegmentId());
        assertEquals(id, state.getLowerSegmentId());
    }
}
