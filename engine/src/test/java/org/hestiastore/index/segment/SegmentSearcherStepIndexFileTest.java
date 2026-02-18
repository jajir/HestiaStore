package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSearcherStepIndexFileTest {

    @Mock
    private SegmentSearcherContext<String, Long> ctx;

    private SegmentSearcherStepIndexFile<String, Long> step;

    @BeforeEach
    void setup() {
        step = new SegmentSearcherStepIndexFile<>();
    }

    @Test
    void stops_when_position_missing() {
        when(ctx.getPositionFromScarceIndex()).thenReturn(null);
        final var res = new SegmentSearcherResult<Long>();

        final boolean cont = step.filter(ctx, res);

        assertFalse(cont);
        assertNull(res.getValue());
    }

    @Test
    void stops_and_marks_false_positive_when_not_found_in_index() {
        when(ctx.getPositionFromScarceIndex()).thenReturn(10);
        when(ctx.searchInIndex(10)).thenReturn(null);
        final var res = new SegmentSearcherResult<Long>();

        final boolean cont = step.filter(ctx, res);

        assertFalse(cont);
        assertNull(res.getValue());
        verify(ctx).incrementFalsePositive();
    }

    @Test
    void sets_value_when_found_and_stops() {
        when(ctx.getPositionFromScarceIndex()).thenReturn(5);
        when(ctx.searchInIndex(5)).thenReturn(77L);
        final var res = new SegmentSearcherResult<Long>();

        final boolean cont = step.filter(ctx, res);

        assertFalse(cont);
        org.junit.jupiter.api.Assertions.assertEquals(77L, res.getValue());
    }
}
