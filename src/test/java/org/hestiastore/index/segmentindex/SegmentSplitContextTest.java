package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class SegmentSplitContextTest {

    @Test
    void gettersAndSettersReflectValues() {
        final Segment<String, String> segment = mock(Segment.class);
        final SegmentSplitterPlan<String, String> plan = mock(
                SegmentSplitterPlan.class);
        final SegmentId lower = SegmentId.of(1);
        final SegmentId upper = SegmentId.of(2);
        final SegmentWriterTxFactory<String, String> factory = id -> mock(
                WriteTransaction.class);

        final SegmentSplitContext<String, String> context = new SegmentSplitContext<>(
                segment, plan, lower, upper, factory);

        assertSame(segment, context.getSegment());
        assertSame(plan, context.getPlan());
        assertSame(lower, context.getLowerSegmentId());
        assertSame(upper, context.getUpperSegmentId());
        assertSame(factory, context.getWriterTxFactory());

        final Segment<String, String> newSegment = mock(Segment.class);
        final SegmentSplitterPlan<String, String> newPlan = mock(
                SegmentSplitterPlan.class);
        final SegmentId newLower = SegmentId.of(3);
        final SegmentId newUpper = SegmentId.of(4);
        final SegmentWriterTxFactory<String, String> newFactory = id -> mock(
                WriteTransaction.class);

        context.setSegment(newSegment);
        context.setPlan(newPlan);
        context.setLowerSegmentId(newLower);
        context.setUpperSegmentId(newUpper);
        context.setWriterTxFactory(newFactory);

        assertSame(newSegment, context.getSegment());
        assertSame(newPlan, context.getPlan());
        assertSame(newLower, context.getLowerSegmentId());
        assertSame(newUpper, context.getUpperSegmentId());
        assertSame(newFactory, context.getWriterTxFactory());
    }
}
