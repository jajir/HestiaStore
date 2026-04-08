package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexOperationCoordinatorTest {

    private final TypeDescriptorShortString typeDescriptor = new TypeDescriptorShortString();

    @Mock
    private DirectSegmentWriteCoordinator<Integer, String> directSegmentWriteCoordinator;

    @Mock
    private DirectSegmentReadCoordinator<Integer, String> directSegmentReadCoordinator;

    @Mock
    private IndexWalCoordinator<Integer, String> walCoordinator;

    @Mock
    private IndexRetryPolicy retryPolicy;

    private Stats stats;
    private IndexOperationCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        stats = new Stats();
        coordinator = new IndexOperationCoordinator<>(typeDescriptor, stats,
                directSegmentWriteCoordinator, directSegmentReadCoordinator,
                walCoordinator, retryPolicy);
    }

    @Test
    void putRetriesBusyWriteAndRecordsAppliedWalLsn() {
        when(retryPolicy.startNanos()).thenReturn(1L);
        when(walCoordinator.appendPut(1, "one")).thenReturn(7L);
        when(directSegmentWriteCoordinator.put(1, "one"))
                .thenReturn(IndexResult.busy(), IndexResult.ok());

        coordinator.put(1, "one");

        assertEquals(1L, stats.getPutCx());
        assertEquals(1L, stats.getPutBusyRetryCx());
        assertEquals(0L, stats.getPutBusyTimeoutCx());
        verify(retryPolicy).backoffOrThrow(1L, "put", null);
        verify(walCoordinator).recordAppliedLsn(7L);
    }

    @Test
    void putBusyTimeoutIncrementsTimeoutMetrics() {
        when(retryPolicy.startNanos()).thenReturn(1L);
        when(walCoordinator.appendPut(1, "one")).thenReturn(7L);
        when(directSegmentWriteCoordinator.put(1, "one"))
                .thenReturn(IndexResult.busy());
        final IndexException timeout = new IndexException(
                "Index operation 'put' timed out after 30 ms");
        org.mockito.Mockito.doThrow(timeout).when(retryPolicy)
                .backoffOrThrow(1L, "put", null);

        final IndexException thrown = assertThrows(IndexException.class,
                () -> coordinator.put(1, "one"));

        assertEquals(timeout, thrown);
        assertEquals(1L, stats.getPutCx());
        assertEquals(1L, stats.getPutBusyRetryCx());
        assertEquals(1L, stats.getPutBusyTimeoutCx());
        assertTrue(stats.getPutBusyWaitP95Micros() >= 0L);
    }

    @Test
    void getRetriesClosedResultUntilReadSucceeds() {
        when(retryPolicy.startNanos()).thenReturn(1L);
        when(directSegmentReadCoordinator.get(1))
                .thenReturn(IndexResult.closed(), IndexResult.ok("one"));

        assertEquals("one", coordinator.get(1));

        assertEquals(1L, stats.getGetCx());
        verify(retryPolicy).backoffOrThrow(1L, "get", null);
    }

    @Test
    void replayWalRecordUsesTombstoneForDeleteOperation() {
        when(retryPolicy.startNanos()).thenReturn(1L);
        @SuppressWarnings("unchecked")
        final WalRuntime.ReplayRecord<Integer, String> replayRecord = (WalRuntime.ReplayRecord<Integer, String>) org.mockito.Mockito
                .mock(WalRuntime.ReplayRecord.class);
        when(replayRecord.getOperation()).thenReturn(WalRuntime.Operation.DELETE);
        when(replayRecord.getKey()).thenReturn(3);
        when(replayRecord.getLsn()).thenReturn(11L);
        when(directSegmentWriteCoordinator.put(3,
                TypeDescriptorShortString.TOMBSTONE_VALUE))
                        .thenReturn(IndexResult.ok());

        coordinator.replayWalRecord(replayRecord);

        verify(directSegmentWriteCoordinator).put(3,
                TypeDescriptorShortString.TOMBSTONE_VALUE);
        verify(walCoordinator).recordAppliedLsn(11L);
    }

    @Test
    void putRejectsTombstoneValues() {
        assertThrows(IllegalArgumentException.class,
                () -> coordinator.put(1,
                        TypeDescriptorShortString.TOMBSTONE_VALUE));
    }
}
