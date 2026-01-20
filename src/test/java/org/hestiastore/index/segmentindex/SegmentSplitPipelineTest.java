package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.NoSuchElementException;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Filter;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitPipelineTest {

    @Mock
    private Segment<String, String> segment;
    @Mock
    private SegmentSplitterPlan<String, String> plan;

    private SegmentSplitContext<String, String> context;

    @BeforeEach
    void setUp() {
        context = new SegmentSplitContext<>(segment, plan, SegmentId.of(1),
                SegmentId.of(2), segmentId -> null);
    }

    @AfterEach
    void tearDown() {
        context = null;
    }

    @Nested
    class RunClosesIteratorAndReturnsResult {

        private SegmentSplitPipeline<String, String> pipeline;
        private ClosingIterator iterator;
        private SegmentSplitterResult<String, String> result;

        @BeforeEach
        void setUp() {
            iterator = new ClosingIterator();
            result = new SegmentSplitterResult<>(SegmentId.of(1), "a", "z",
                    SegmentSplitterResult.SegmentSplittingStatus.SPLIT);
            final Filter<SegmentSplitContext<String, String>, SegmentSplitState<String, String>> step = (
                    ctx, state) -> {
                state.setIterator(iterator);
                state.setResult(result);
                return false;
            };
            pipeline = new SegmentSplitPipeline<>(List.of(step));
        }

        @AfterEach
        void tearDown() {
            pipeline = null;
            iterator = null;
            result = null;
        }

        @Test
        void runClosesIteratorAndReturnsResult() {
            final SegmentSplitterResult<String, String> out = pipeline
                    .run(context);

            assertSame(result, out);
            assertTrue(iterator.closed);
        }
    }

    @Nested
    class RunThrowsWhenNoResultProvided {

        private SegmentSplitPipeline<String, String> pipeline;

        @BeforeEach
        void setUp() {
            final Filter<SegmentSplitContext<String, String>, SegmentSplitState<String, String>> step = (
                    ctx, state) -> true;
            pipeline = new SegmentSplitPipeline<>(List.of(step));
        }

        @AfterEach
        void tearDown() {
            pipeline = null;
        }

        @Test
        void runThrowsWhenNoResultProvided() {
            assertThrows(IllegalStateException.class,
                    () -> pipeline.run(context));
        }
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
