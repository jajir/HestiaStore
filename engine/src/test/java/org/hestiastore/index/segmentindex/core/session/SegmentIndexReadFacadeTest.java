package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.streaming.SegmentIndexEntryIteratorDecorator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class SegmentIndexReadFacadeTest {

    private SegmentIndexDataAccess<Integer, String> dataAccess;
    private SegmentIndexEntryIteratorDecorator<Integer, String> iteratorDecorator;
    private SegmentIndexReadFacade<Integer, String> readFacade;

    @BeforeEach
    void setUp() {
        dataAccess = mock(SegmentIndexDataAccess.class);
        iteratorDecorator = mock(SegmentIndexEntryIteratorDecorator.class);
        readFacade = new SegmentIndexReadFacade<>(
                new SegmentIndexTrackedOperationRunner<>(() -> {
                    // Test guard allows all tracked operations immediately.
                },
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

}
