package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
class SegmentIndexDataAccessImplTest {

    @Mock
    private SegmentIndexOperationAccess<Integer, String> operationAccess;

    @Mock
    private SegmentTopologyRuntimeAccess<Integer, String> topologyRuntime;

    private SegmentIndexDataAccessImpl<Integer, String> dataAccess;

    @BeforeEach
    void setUp() {
        dataAccess = new SegmentIndexDataAccessImpl<>(operationAccess,
                topologyRuntime);
    }

    @Test
    void constructorRejectsNullOperationAccess() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentIndexDataAccessImpl<>(null, topologyRuntime));

        assertEquals("Property 'operationAccess' must not be null.",
                ex.getMessage());
    }

    @Test
    void pointOperationsDelegateToOperationAccess() {
        when(operationAccess.get(1)).thenReturn("one");

        dataAccess.put(1, "one");
        assertEquals("one", dataAccess.get(1));
        dataAccess.delete(1);

        verify(operationAccess).put(1, "one");
        verify(operationAccess).get(1);
        verify(operationAccess).delete(1);
    }

    @Test
    void iteratorOperationsDelegateToTopologyRuntime() {
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentWindow segmentWindow = SegmentWindow.unbounded();
        final EntryIterator<Integer, String> segmentIterator =
                mock(EntryIterator.class);
        final EntryIterator<Integer, String> windowIterator =
                mock(EntryIterator.class);
        when(topologyRuntime.openSegmentIterator(segmentId,
                SegmentIteratorIsolation.FAIL_FAST)).thenReturn(segmentIterator);
        when(topologyRuntime.openWindowIterator(segmentWindow,
                SegmentIteratorIsolation.FAIL_FAST)).thenReturn(windowIterator);

        assertSame(segmentIterator, dataAccess.openSegmentIterator(segmentId,
                SegmentIteratorIsolation.FAIL_FAST));
        assertSame(windowIterator, dataAccess.openWindowIterator(segmentWindow,
                SegmentIteratorIsolation.FAIL_FAST));

        verify(topologyRuntime).openSegmentIterator(segmentId,
                SegmentIteratorIsolation.FAIL_FAST);
        verify(topologyRuntime).openWindowIterator(segmentWindow,
                SegmentIteratorIsolation.FAIL_FAST);
    }
}
