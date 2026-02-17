package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertSame;

import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitContextTest {

    @Mock
    private Segment<String, String> segment;
    @Mock
    private SegmentSplitterPlan<String, String> plan;
    @Mock
    private Segment<String, String> newSegment;
    @Mock
    private SegmentSplitterPlan<String, String> newPlan;
    @Mock
    private WriteTransaction<String, String> writeTransaction;
    @Mock
    private WriteTransaction<String, String> newWriteTransaction;

    private SegmentSplitContext<String, String> context;
    private SegmentWriterTxFactory<String, String> factory;
    private SegmentId lower;
    private SegmentId upper;

    @BeforeEach
    void setUp() {
        lower = SegmentId.of(1);
        upper = SegmentId.of(2);
        factory = id -> writeTransaction;
        context = new SegmentSplitContext<>(segment, plan, lower, upper,
                factory);
    }

    @AfterEach
    void tearDown() {
        context = null;
        factory = null;
        lower = null;
        upper = null;
    }

    @Test
    void gettersAndSettersReflectValues() {
        assertSame(segment, context.getSegment());
        assertSame(plan, context.getPlan());
        assertSame(lower, context.getLowerSegmentId());
        assertSame(upper, context.getUpperSegmentId());
        assertSame(factory, context.getWriterTxFactory());

        final SegmentId newLower = SegmentId.of(3);
        final SegmentId newUpper = SegmentId.of(4);
        final SegmentWriterTxFactory<String, String> newFactory = id -> newWriteTransaction;

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
