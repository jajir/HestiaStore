package org.hestiastore.index.segmentindex.core.maintenance;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.Snapshot;
import org.hestiastore.index.segmentindex.core.routing.BackgroundSplitCoordinator;
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
    private BackgroundSplitCoordinator<Integer, String> backgroundSplitCoordinator;

    @Mock
    private BackgroundSplitPolicyLoop<Integer, String> backgroundSplitPolicyLoop;

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
                backgroundSplitCoordinator, backgroundSplitPolicyLoop,
                stableSegmentCoordinator, walCoordinator);
    }

    @Test
    void compact_skipsStableCompactionWhileSplitIsStillRunning() {
        when(backgroundSplitCoordinator.splitInFlightCount()).thenReturn(1);

        coordinator.compact();

        verifyNoInteractions(stableSegmentCoordinator, backgroundSplitPolicyLoop);
    }

    @Test
    void compact_compactsMappedSegmentsWhenRuntimeIsSettled() {
        final SegmentId segmentId = SegmentId.of(7);
        when(backgroundSplitCoordinator.splitInFlightCount()).thenReturn(0);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        runPausedActionImmediately();

        coordinator.compact();

        verify(stableSegmentCoordinator).compactSegment(segmentId, false);
        verify(backgroundSplitPolicyLoop).scheduleScanIfIdle();
    }

    @Test
    void flushAndWait_retriesStableFlushWhenTopologyChanges() {
        when(keyToSegmentMap.snapshot()).thenReturn(snapshot);
        when(snapshot.version()).thenReturn(11L);
        when(keyToSegmentMap.isAtVersion(anyLong())).thenReturn(false);

        coordinator.flushAndWait();

        verify(backgroundSplitPolicyLoop, times(2)).awaitExhausted();
        verify(stableSegmentCoordinator, times(2)).flushMappedSegmentsAndWait();
        verify(backgroundSplitPolicyLoop).scheduleScanIfIdle();
        verify(keyToSegmentMap).flushIfDirty();
        verify(walCoordinator).checkpoint();
    }

    private void runPausedActionImmediately() {
        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(backgroundSplitCoordinator).runWithSplitSchedulingPaused(any(Runnable.class));
    }
}
