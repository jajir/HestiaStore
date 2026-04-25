package org.hestiastore.index.segmentindex.core.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
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
    private DirectSegmentAccess<Integer, String> directSegmentCoordinator;

    @Mock
    private IndexWalCoordinator<Integer, String> walCoordinator;

    private Stats stats;
    private IndexOperationCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        stats = new Stats();
        coordinator = new IndexOperationCoordinator<>(typeDescriptor, stats,
                directSegmentCoordinator, walCoordinator);
    }

    @Test
    void putWritesDirectlyAndRecordsAppliedWalLsn() {
        when(walCoordinator.appendPut(1, "one")).thenReturn(7L);

        coordinator.put(1, "one");

        assertEquals(1L, stats.getPutCount());
        verify(directSegmentCoordinator).put(1, "one");
        verify(walCoordinator).recordAppliedLsn(7L);
    }

    @Test
    void getReadsDirectly() {
        when(directSegmentCoordinator.get(1)).thenReturn("one");

        assertEquals("one", coordinator.get(1));

        assertEquals(1L, stats.getGetCount());
        verify(directSegmentCoordinator).get(1);
    }

    @Test
    void deleteWritesTombstoneAndRecordsAppliedWalLsn() {
        when(walCoordinator.appendDelete(1)).thenReturn(8L);

        coordinator.delete(1);

        assertEquals(1L, stats.getDeleteCount());
        verify(directSegmentCoordinator).put(1,
                TypeDescriptorShortString.TOMBSTONE_VALUE);
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
