package org.hestiastore.index.segmentindex.core.streaming;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.session.IndexOperationTrackingAccess;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexDataAccess;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexTrackedOperationRunner;
import org.hestiastore.index.segmentindex.core.session.state.IndexState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentIndexReadFacadeTest {

    private SegmentIndexDataAccess<Integer, String> dataAccess;
    private SegmentIndexEntryIteratorDecorator<Integer, String> iteratorDecorator;
    private SegmentIndexReadFacade<Integer, String> readFacade;

    @BeforeEach
    void setUp() {
        dataAccess = mock(SegmentIndexDataAccess.class);
        iteratorDecorator = mock(SegmentIndexEntryIteratorDecorator.class);
        readFacade = new SegmentIndexReadFacade<>(
                new SegmentIndexTrackedOperationRunner<>(this::readyState,
                        IndexOperationTrackingAccess.create()),
                dataAccess, iteratorDecorator);
    }

    @Test
    void iteratorReadsDelegateToSpecificRuntimeCollaborators() {
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentWindow window = SegmentWindow.unbounded();
        final EntryIterator<Integer, String> stableIterator = mock(
                EntryIterator.class);
        final EntryIterator<Integer, String> windowIterator = mock(
                EntryIterator.class);
        final EntryIterator<Integer, String> decoratedIterator = mock(
                EntryIterator.class);
        when(dataAccess.openSegmentIterator(segmentId,
                SegmentIteratorIsolation.FAIL_FAST)).thenReturn(stableIterator);
        when(dataAccess.openWindowIterator(window,
                SegmentIteratorIsolation.FAIL_FAST)).thenReturn(windowIterator);
        when(iteratorDecorator.decorate(windowIterator))
                .thenReturn(decoratedIterator);

        assertSame(stableIterator, readFacade.openSegmentIterator(segmentId,
                SegmentIteratorIsolation.FAIL_FAST));
        assertSame(decoratedIterator,
                readFacade.openWindowIterator(window,
                        SegmentIteratorIsolation.FAIL_FAST));

        verify(dataAccess).openSegmentIterator(segmentId,
                SegmentIteratorIsolation.FAIL_FAST);
        verify(dataAccess).openWindowIterator(window,
                SegmentIteratorIsolation.FAIL_FAST);
        verify(iteratorDecorator).decorate(windowIterator);
    }

    private IndexState<Integer, String> readyState() {
        return new IndexState<>() {
            @Override
            public SegmentIndexState state() {
                return SegmentIndexState.READY;
            }

            @Override
            public IndexState<Integer, String> onReady() {
                return this;
            }

            @Override
            public IndexState<Integer, String> onClose() {
                return this;
            }

            @Override
            public IndexState<Integer, String> finishClose() {
                return this;
            }

            @Override
            public void tryPerformOperation() {
                // The test state allows all tracked operations immediately.
            }
        };
    }
}
