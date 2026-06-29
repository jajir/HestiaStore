package org.hestiastore.index.segmentindex.core.storage;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentindex.routemap.RouteMapSnapshot;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RouteMapConsistencyCheckerTest {

    @Mock
    private BlockingSegment<Integer, String> segmentHandle;
    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;
    @Mock
    private SegmentRouteMap<Integer> keyToSegmentMap;
    @Mock
    private RouteMapSnapshot<Integer> snapshot;
    @Mock
    private EntryIterator<Integer, String> iterator;

    private RouteMapConsistencyChecker<Integer, String> checker;

    @BeforeEach
    void setUp() {
        when(keyToSegmentMap.snapshot()).thenReturn(snapshot);
        checker = new RouteMapConsistencyChecker<>(keyToSegmentMap,
                segmentRegistry);
    }

    @AfterEach
    void tearDown() {
        checker = null;
    }

    @Test
    void emptySegmentIsRemovedAfterIsolationCheck() {
        when(segmentHandle.checkAndRepairConsistency()).thenReturn(null);
        when(segmentHandle.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(false);
        when(snapshot.getSegmentIds(SegmentWindow.unbounded()))
                .thenReturn(List.of(SegmentId.of(1)));
        when(segmentRegistry.loadSegment(SegmentId.of(1)))
                .thenReturn(segmentHandle);

        checker.checkAndRepairConsistency();

        verify(keyToSegmentMap).removeSegmentRoute(SegmentId.of(1));
        verify(keyToSegmentMap).flushIfDirty();
        verify(segmentRegistry).deleteSegment(SegmentId.of(1));
    }

    @Test
    void segmentLoad_usesLoadedSegment() {
        when(segmentHandle.checkAndRepairConsistency()).thenReturn(10);
        when(snapshot.getSegmentIds(SegmentWindow.unbounded()))
                .thenReturn(List.of(SegmentId.of(1)));
        when(snapshot.findSegmentIdForKey(10)).thenReturn(SegmentId.of(1));
        when(segmentRegistry.loadSegment(SegmentId.of(1)))
                .thenReturn(segmentHandle);

        checker = new RouteMapConsistencyChecker<>(keyToSegmentMap,
                segmentRegistry);

        checker.checkAndRepairConsistency();
    }

    @Test
    void segmentLoad_wrapsLoadFailure() {
        when(snapshot.getSegmentIds(SegmentWindow.unbounded()))
                .thenReturn(List.of(SegmentId.of(1)));
        when(segmentRegistry.loadSegment(SegmentId.of(1))).thenThrow(
                new IndexException("loadSegmentForConsistency timed out"));

        checker = new RouteMapConsistencyChecker<>(keyToSegmentMap,
                segmentRegistry);

        final IndexException ex = assertThrows(IndexException.class,
                () -> checker.checkAndRepairConsistency());

        assertTrue(ex.getMessage()
                .contains("Segment 'segment-00001' is not found in index."));
    }
}
