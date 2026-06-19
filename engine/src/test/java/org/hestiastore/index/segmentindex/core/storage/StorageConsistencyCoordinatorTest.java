package org.hestiastore.index.segmentindex.core.storage;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentindex.routemap.RouteMapSnapshot;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StorageConsistencyCoordinatorTest {

    private static final SegmentId MATCHED_SEGMENT_ID = SegmentId.of(7);
    private static final SegmentId OTHER_SEGMENT_ID = SegmentId.of(8);

    @Mock
    private SegmentRouteMap<Integer> keyToSegmentMap;
    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;
    @Mock
    private SegmentDirectoryRecoveryScanner<Integer> segmentDirectoryInspector;
    @Mock
    private OrphanedSegmentCleaner<Integer, String> orphanedSegmentDirectoryRemover;
    @Mock
    private RouteMapSnapshot<Integer> snapshot;
    @Mock
    private BlockingSegment<Integer, String> segment;

    private StorageConsistencyCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        coordinator = new StorageConsistencyCoordinator<>(keyToSegmentMap,
                segmentRegistry, segmentDirectoryInspector,
                orphanedSegmentDirectoryRemover);
    }

    @Test
    void checkAndRepairConsistency_checksSegmentsAndCleansOrphans() {
        when(keyToSegmentMap.snapshot()).thenReturn(snapshot);
        when(snapshot.getSegmentIds(SegmentWindow.unbounded()))
                .thenReturn(List.of(MATCHED_SEGMENT_ID));
        when(segmentRegistry.loadSegment(MATCHED_SEGMENT_ID))
                .thenReturn(segment);
        when(segment.checkAndRepairConsistency()).thenReturn(10);
        when(snapshot.findSegmentIdForKey(10)).thenReturn(MATCHED_SEGMENT_ID);
        when(segmentDirectoryInspector.discoverOrphanedSegmentDirectories())
                .thenReturn(List.of(OTHER_SEGMENT_ID));

        coordinator.checkAndRepairConsistency();

        final InOrder order = inOrder(keyToSegmentMap,
                segmentDirectoryInspector, orphanedSegmentDirectoryRemover);
        order.verify(keyToSegmentMap).validateUniqueSegmentIds();
        order.verify(segmentDirectoryInspector)
                .discoverOrphanedSegmentDirectories();
        order.verify(orphanedSegmentDirectoryRemover).remove(OTHER_SEGMENT_ID);
    }

    @Test
    void runStartupConsistencyCheck_onlyChecksLockedSegments() {
        when(keyToSegmentMap.snapshot()).thenReturn(snapshot);
        when(snapshot.getSegmentIds(SegmentWindow.unbounded()))
                .thenReturn(List.of(MATCHED_SEGMENT_ID, OTHER_SEGMENT_ID));
        when(segmentDirectoryInspector.hasSegmentLockFile(MATCHED_SEGMENT_ID))
                .thenReturn(true);
        when(segmentDirectoryInspector.hasSegmentLockFile(OTHER_SEGMENT_ID))
                .thenReturn(false);
        when(segmentRegistry.loadSegment(MATCHED_SEGMENT_ID))
                .thenReturn(segment);
        when(segment.checkAndRepairConsistency()).thenReturn(10);
        when(snapshot.findSegmentIdForKey(10)).thenReturn(MATCHED_SEGMENT_ID);
        when(segmentDirectoryInspector.discoverOrphanedSegmentDirectories())
                .thenReturn(List.of());

        coordinator.runStartupConsistencyCheck();

        verify(segmentRegistry).loadSegment(MATCHED_SEGMENT_ID);
        verify(segmentRegistry, never()).loadSegment(OTHER_SEGMENT_ID);
    }

    @Test
    void runStartupConsistencyCheck_failureDoesNotAffectNormalConsistencyCheck() {
        final RuntimeException failure = new RuntimeException("boom");
        doThrow(failure).doNothing().when(keyToSegmentMap)
                .validateUniqueSegmentIds();
        when(keyToSegmentMap.snapshot()).thenReturn(snapshot);
        when(snapshot.getSegmentIds(SegmentWindow.unbounded()))
                .thenReturn(List.of(OTHER_SEGMENT_ID));
        when(segmentRegistry.loadSegment(OTHER_SEGMENT_ID)).thenReturn(segment);
        when(segment.checkAndRepairConsistency()).thenReturn(20);
        when(snapshot.findSegmentIdForKey(20)).thenReturn(OTHER_SEGMENT_ID);
        when(segmentDirectoryInspector.discoverOrphanedSegmentDirectories())
                .thenReturn(List.of());

        final RuntimeException ex = assertThrows(RuntimeException.class,
                coordinator::runStartupConsistencyCheck);
        assertSame(failure, ex);

        coordinator.checkAndRepairConsistency();

        verify(segmentRegistry).loadSegment(OTHER_SEGMENT_ID);
    }
}
