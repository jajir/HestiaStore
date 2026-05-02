package org.hestiastore.index.segmentindex.core.operations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.metrics.Stats;
import org.hestiastore.index.segmentindex.core.segmentaccess.SegmentAccess;
import org.hestiastore.index.segmentindex.core.segmentaccess.SegmentAccessService;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexOperationCoordinatorTest {

    private final TypeDescriptorShortString typeDescriptor =
            new TypeDescriptorShortString();

    @Mock
    private SegmentAccessService<Integer, String> segmentAccessService;

    @Mock
    private IndexWalCoordinator<Integer, String> walCoordinator;

    @Mock
    private SegmentAccess<Integer, String> segmentAccess;

    @Mock
    private BlockingSegment<Integer, String> blockingSegment;

    private Stats stats;
    private IndexOperationCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        stats = new Stats();
        coordinator = new IndexOperationCoordinator<>(typeDescriptor, stats,
                segmentAccessService, walCoordinator);
    }

    @Test
    void putWritesThroughSegmentAccessServiceAndRecordsAppliedWalLsn() {
        when(walCoordinator.appendPut(1, "one")).thenReturn(7L);
        when(segmentAccessService.acquireForWrite(1)).thenReturn(segmentAccess);
        when(segmentAccess.segment()).thenReturn(blockingSegment);

        coordinator.put(1, "one");

        assertEquals(1L, stats.getPutCount());
        verify(blockingSegment).put(1, "one");
        verify(segmentAccess).close();
        verify(walCoordinator).recordAppliedLsn(7L);
    }

    @Test
    void getReadsThroughSegmentAccessService() {
        when(segmentAccessService.acquireForRead(1)).thenReturn(segmentAccess);
        when(segmentAccess.segment()).thenReturn(blockingSegment);
        when(blockingSegment.get(1)).thenReturn("one");

        assertEquals("one", coordinator.get(1));

        assertEquals(1L, stats.getGetCount());
        verify(blockingSegment).get(1);
        verify(segmentAccess).close();
    }

    @Test
    void getReturnsNullWhenKeyHasNoRoute() {
        when(segmentAccessService.acquireForRead(1)).thenReturn(null);

        assertNull(coordinator.get(1));

        assertEquals(1L, stats.getGetCount());
    }

    @Test
    void deleteWritesTombstoneThroughSegmentAccessServiceAndRecordsAppliedWalLsn() {
        when(walCoordinator.appendDelete(1)).thenReturn(8L);
        when(segmentAccessService.acquireForWrite(1)).thenReturn(segmentAccess);
        when(segmentAccess.segment()).thenReturn(blockingSegment);

        coordinator.delete(1);

        assertEquals(1L, stats.getDeleteCount());
        verify(blockingSegment).put(1,
                TypeDescriptorShortString.TOMBSTONE_VALUE);
        verify(segmentAccess).close();
        verify(walCoordinator).recordAppliedLsn(8L);
    }

    @Test
    void replayWalRecordUsesTombstoneForDeleteOperation() {
        @SuppressWarnings("unchecked")
        final WalRuntime.ReplayRecord<Integer, String> replayRecord =
                (WalRuntime.ReplayRecord<Integer, String>) org.mockito.Mockito
                        .mock(WalRuntime.ReplayRecord.class);
        when(replayRecord.getOperation()).thenReturn(
                WalRuntime.Operation.DELETE);
        when(replayRecord.getKey()).thenReturn(3);
        when(replayRecord.getLsn()).thenReturn(11L);
        when(segmentAccessService.acquireForWrite(3)).thenReturn(segmentAccess);
        when(segmentAccess.segment()).thenReturn(blockingSegment);

        coordinator.replayWalRecord(replayRecord);

        verify(blockingSegment).put(3,
                TypeDescriptorShortString.TOMBSTONE_VALUE);
        verify(segmentAccess).close();
        verify(walCoordinator).recordAppliedLsn(11L);
    }

    @Test
    void putRejectsTombstoneValues() {
        assertThrows(IllegalArgumentException.class,
                () -> coordinator.put(1,
                        TypeDescriptorShortString.TOMBSTONE_VALUE));
    }
}
