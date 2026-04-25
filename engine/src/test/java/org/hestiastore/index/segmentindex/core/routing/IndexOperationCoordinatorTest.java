package org.hestiastore.index.segmentindex.core.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
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
    private DirectSegmentAccess<Integer, String> directSegmentCoordinator;

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
                directSegmentCoordinator,
                walCoordinator, retryPolicy);
    }

    @Test
    void putRetriesBusyWriteAndRecordsAppliedWalLsn() {
        when(retryPolicy.startNanos()).thenReturn(1L);
        when(walCoordinator.appendPut(1, "one")).thenReturn(7L);
        when(directSegmentCoordinator.put(1, "one"))
                .thenReturn(IndexResult.busy(), IndexResult.ok());

        coordinator.put(1, "one");

        assertEquals(1L, stats.getPutCount());
        assertEquals(1L, stats.getPutBusyRetryCount());
        assertEquals(0L, stats.getPutBusyTimeoutCount());
        verify(retryPolicy).backoffOrThrow(1L, "put", null);
        verify(walCoordinator).recordAppliedLsn(7L);
    }

    @Test
    void putRetriesClosedWriteAndRecordsAppliedWalLsn() {
        when(retryPolicy.startNanos()).thenReturn(1L);
        when(walCoordinator.appendPut(1, "one")).thenReturn(7L);
        when(directSegmentCoordinator.put(1, "one"))
                .thenReturn(IndexResult.closed(), IndexResult.ok());

        coordinator.put(1, "one");

        assertEquals(1L, stats.getPutCount());
        assertEquals(0L, stats.getPutBusyRetryCount());
        verify(retryPolicy).backoffOrThrow(1L, "put", null);
        verify(walCoordinator).recordAppliedLsn(7L);
    }

    @Test
    void putBusyTimeoutIncrementsTimeoutMetrics() {
        when(retryPolicy.startNanos()).thenReturn(1L);
        when(walCoordinator.appendPut(1, "one")).thenReturn(7L);
        when(directSegmentCoordinator.put(1, "one"))
                .thenReturn(IndexResult.busy());
        final IndexException timeout = new IndexException(
                "Index operation 'put' timed out after 30 ms");
        org.mockito.Mockito.doThrow(timeout).when(retryPolicy)
                .backoffOrThrow(1L, "put", null);

        final IndexException thrown = assertThrows(IndexException.class,
                () -> coordinator.put(1, "one"));

        assertEquals(timeout, thrown);
        assertEquals(1L, stats.getPutCount());
        assertEquals(1L, stats.getPutBusyRetryCount());
        assertEquals(1L, stats.getPutBusyTimeoutCount());
        assertTrue(stats.getPutBusyWaitP95Micros() >= 0L);
    }

    @Test
    void getRetriesClosedResultUntilReadSucceeds() {
        when(retryPolicy.startNanos()).thenReturn(1L);
        when(directSegmentCoordinator.get(1))
                .thenReturn(IndexResult.closed(), IndexResult.ok("one"));

        assertEquals("one", coordinator.get(1));

        assertEquals(1L, stats.getGetCount());
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
        when(directSegmentCoordinator.put(3,
                TypeDescriptorShortString.TOMBSTONE_VALUE))
                        .thenReturn(IndexResult.ok());

        coordinator.replayWalRecord(replayRecord);

        verify(directSegmentCoordinator).put(3,
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
