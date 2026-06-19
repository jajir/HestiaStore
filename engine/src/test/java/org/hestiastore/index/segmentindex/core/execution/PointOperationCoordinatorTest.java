package org.hestiastore.index.segmentindex.core.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.core.routing.MappedSegmentLease;
import org.hestiastore.index.segmentindex.core.routing.MappedSegmentLeaseService;
import org.hestiastore.index.segmentindex.core.storage.StorageCoordinator;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PointOperationCoordinatorTest {

    private final TypeDescriptorShortString typeDescriptor =
            new TypeDescriptorShortString();

    @Mock
    private MappedSegmentLeaseService<Integer, String> segmentLeaseService;

    @Mock
    private StorageCoordinator<Integer, String> storageService;

    @Mock
    private MappedSegmentLease<Integer, String> segmentLease;

    @Mock
    private BlockingSegment<Integer, String> blockingSegment;

    private IndexOperationStatsRecorder statsRecorder;
    private PointOperationCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        statsRecorder = new IndexOperationStatsRecorder();
        coordinator = new PointOperationCoordinator<>(typeDescriptor,
                statsRecorder, segmentLeaseService, storageService);
    }

    @Test
    void putWritesThroughSegmentLeaseServiceAndRecordsAppliedWalLsn() {
        when(storageService.appendWalPut(1, "one")).thenReturn(7L);
        when(segmentLeaseService.acquireForWrite(1)).thenReturn(segmentLease);
        when(segmentLease.segment()).thenReturn(blockingSegment);

        coordinator.put(1, "one");

        assertEquals(1L, statsRecorder.statsSnapshot().getPutCount());
        verify(blockingSegment).put(1, "one");
        verify(segmentLease).close();
        verify(storageService).recordAppliedWalLsn(7L);
    }

    @Test
    void getReadsThroughSegmentLeaseService() {
        when(segmentLeaseService.acquireForRead(1)).thenReturn(segmentLease);
        when(segmentLease.segment()).thenReturn(blockingSegment);
        when(blockingSegment.get(1)).thenReturn("one");

        assertEquals("one", coordinator.get(1));

        assertEquals(1L, statsRecorder.statsSnapshot().getGetCount());
        verify(blockingSegment).get(1);
        verify(segmentLease).close();
    }

    @Test
    void getReturnsNullWhenKeyHasNoRoute() {
        when(segmentLeaseService.acquireForRead(1)).thenReturn(null);

        assertNull(coordinator.get(1));

        assertEquals(1L, statsRecorder.statsSnapshot().getGetCount());
    }

    @Test
    void deleteWritesTombstoneThroughSegmentLeaseServiceAndRecordsAppliedWalLsn() {
        when(storageService.appendWalDelete(1)).thenReturn(8L);
        when(segmentLeaseService.acquireForWrite(1)).thenReturn(segmentLease);
        when(segmentLease.segment()).thenReturn(blockingSegment);

        coordinator.delete(1);

        assertEquals(1L, statsRecorder.statsSnapshot().getDeleteCount());
        verify(blockingSegment).put(1,
                TypeDescriptorShortString.TOMBSTONE_VALUE);
        verify(segmentLease).close();
        verify(storageService).recordAppliedWalLsn(8L);
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
        when(segmentLeaseService.acquireForWrite(3)).thenReturn(segmentLease);
        when(segmentLease.segment()).thenReturn(blockingSegment);

        coordinator.replayWalRecord(replayRecord);

        verify(blockingSegment).put(3,
                TypeDescriptorShortString.TOMBSTONE_VALUE);
        verify(segmentLease).close();
        verify(storageService).recordAppliedWalLsn(11L);
    }

    @Test
    void putRejectsTombstoneValues() {
        assertThrows(IllegalArgumentException.class,
                () -> coordinator.put(1,
                        TypeDescriptorShortString.TOMBSTONE_VALUE));
    }
}
