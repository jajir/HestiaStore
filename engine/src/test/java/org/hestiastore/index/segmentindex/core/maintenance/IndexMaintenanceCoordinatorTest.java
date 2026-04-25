package org.hestiastore.index.segmentindex.core.maintenance;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.Snapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexMaintenanceCoordinatorTest {

    @Mock
    private KeyToSegmentMap<Integer> keyToSegmentMap;

    @Mock
    private SplitMaintenanceSynchronization<Integer, String> splitSynchronization;

    @Mock
    private StableSegmentCoordinator<Integer, String> stableSegmentCoordinator;

    @Mock
    private IndexWalCoordinator<Integer, String> walCoordinator;

    @Mock
    private Snapshot<Integer> snapshot;

    private IndexMaintenanceCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        coordinator = new IndexMaintenanceCoordinator<>(keyToSegmentMap,
                splitSynchronization,
                stableSegmentCoordinator, walCoordinator);
    }

    @Test
    void compact_compactsMappedSegments() {
        final SegmentId segmentId = SegmentId.of(7);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));

        coordinator.compact();

        verify(stableSegmentCoordinator).compactSegment(segmentId, false);
    }

    @Test
    void flushAndWait_retriesStableFlushWhenTopologyChanges() {
        when(keyToSegmentMap.snapshot()).thenReturn(snapshot);
        when(snapshot.version()).thenReturn(11L);
        when(keyToSegmentMap.isAtVersion(anyLong())).thenReturn(false);

        coordinator.flushAndWait();

        verify(splitSynchronization, times(2)).awaitQuiescence();
        verify(stableSegmentCoordinator, times(2)).flushMappedSegmentsAndWait();
        verify(keyToSegmentMap).flushIfDirty();
        verify(walCoordinator).checkpoint();
    }
}
