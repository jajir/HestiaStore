package org.hestiastore.index.segmentindex.core;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.partition.PartitionRuntime;
import org.hestiastore.index.segmentindex.partition.PartitionRuntimeSnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexMaintenanceCoordinatorTest {

    @Mock
    private KeyToSegmentMapSynchronizedAdapter<Integer> keyToSegmentMap;

    @Mock
    private PartitionRuntime<Integer, String> partitionRuntime;

    @Mock
    private PartitionDrainCoordinator<Integer, String> partitionDrainCoordinator;

    @Mock
    private BackgroundSplitCoordinator<Integer, String> backgroundSplitCoordinator;

    @Mock
    private BackgroundSplitPolicyLoop<Integer, String> backgroundSplitPolicyLoop;

    @Mock
    private StableSegmentCoordinator<Integer, String> stableSegmentCoordinator;

    @Mock
    private IndexWalCoordinator<Integer, String> walCoordinator;

    @Mock
    private KeyToSegmentMap.Snapshot<Integer> snapshot;

    private IndexMaintenanceCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        coordinator = new IndexMaintenanceCoordinator<>(keyToSegmentMap,
                partitionRuntime, partitionDrainCoordinator,
                backgroundSplitCoordinator, backgroundSplitPolicyLoop,
                stableSegmentCoordinator, walCoordinator);
    }

    @Test
    void compact_skipsStableCompactionWhileOverlayStillHasBufferedData() {
        when(partitionRuntime.snapshot())
                .thenReturn(new PartitionRuntimeSnapshot(1, 1, 0, 0, 1, 0, 0,
                        0, 0));

        coordinator.compact();

        verify(partitionDrainCoordinator).drainPartitions(false);
        verifyNoInteractions(stableSegmentCoordinator, backgroundSplitPolicyLoop);
        verify(backgroundSplitCoordinator, never()).splitInFlightCount();
    }

    @Test
    void compact_compactsMappedSegmentsWhenRuntimeIsSettled() {
        final SegmentId segmentId = SegmentId.of(7);
        when(partitionRuntime.snapshot())
                .thenReturn(new PartitionRuntimeSnapshot(1, 0, 0, 0, 0, 0, 0,
                        0, 0));
        when(backgroundSplitCoordinator.splitInFlightCount()).thenReturn(0);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        runPausedActionImmediately();

        coordinator.compact();

        verify(partitionDrainCoordinator).drainPartitions(false);
        verify(stableSegmentCoordinator).compactSegment(segmentId, false);
        verify(backgroundSplitPolicyLoop).scheduleScanIfIdle();
    }

    @Test
    void flushAndWait_retriesStableFlushWhenTopologyChanges() {
        when(keyToSegmentMap.snapshot()).thenReturn(snapshot);
        when(snapshot.version()).thenReturn(11L);
        when(keyToSegmentMap.isVersion(anyLong())).thenReturn(false);

        coordinator.flushAndWait();

        verify(partitionDrainCoordinator, times(3)).drainPartitions(true);
        verify(backgroundSplitPolicyLoop, times(4)).awaitExhausted();
        verify(stableSegmentCoordinator, times(2)).flushMappedSegmentsAndWait();
        verify(backgroundSplitPolicyLoop).scheduleScanIfIdle();
        verify(keyToSegmentMap).optionalyFlush();
        verify(walCoordinator).checkpoint();
    }

    private void runPausedActionImmediately() {
        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(backgroundSplitCoordinator).runWithSplitSchedulingPaused(any(Runnable.class));
    }
}
