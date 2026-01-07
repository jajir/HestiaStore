package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.NoSuchElementException;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Filter;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class SegmentSplitPipelineTest {

    @Test
    void runClosesIteratorAndReturnsResult() {
        final ClosingIterator iterator = new ClosingIterator();
        final SegmentSplitterResult<String, String> result = new SegmentSplitterResult<>(
                SegmentId.of(1), "a", "z",
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);

        final Filter<SegmentSplitContext<String, String>, SegmentSplitState<String, String>> step = (
                ctx, state) -> {
            state.setIterator(iterator);
            state.setResult(result);
            return false;
        };

        final SegmentSplitPipeline<String, String> pipeline = new SegmentSplitPipeline<>(
                List.of(step));
        final SegmentSplitContext<String, String> context = new SegmentSplitContext<>(
                mock(Segment.class), mock(SegmentSplitterPlan.class),
                SegmentId.of(1), SegmentId.of(2),
                segmentId -> null);

        final SegmentSplitterResult<String, String> out = pipeline.run(context);

        assertSame(result, out);
        assertTrue(iterator.closed);
    }

    @Test
    void runThrowsWhenNoResultProvided() {
        final Filter<SegmentSplitContext<String, String>, SegmentSplitState<String, String>> step = (
                ctx, state) -> true;
        final SegmentSplitPipeline<String, String> pipeline = new SegmentSplitPipeline<>(
                List.of(step));
        final SegmentSplitContext<String, String> context = new SegmentSplitContext<>(
                mock(Segment.class), mock(SegmentSplitterPlan.class),
                SegmentId.of(1), SegmentId.of(2),
                segmentId -> null);

        assertThrows(IllegalStateException.class, () -> pipeline.run(context));
    }

    private static final class ClosingIterator
            implements EntryIterator<String, String> {

        private boolean closed;

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Entry<String, String> next() {
            throw new NoSuchElementException();
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean wasClosed() {
            return closed;
        }
    }
}
