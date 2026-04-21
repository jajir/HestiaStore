package org.hestiastore.index.segmentindex.core.durability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.Wal;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
class IndexWalCoordinatorTest {

    @Mock
    private Logger logger;

    @Mock
    private WalRuntime<Integer, String> walRuntime;

    private AtomicLong lastAppliedWalLsn;
    private AtomicInteger drainCalls;
    private AtomicInteger flushCalls;
    private AtomicReference<RuntimeException> handledFailure;
    private IndexWalCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        lastAppliedWalLsn = new AtomicLong(0L);
        drainCalls = new AtomicInteger(0);
        flushCalls = new AtomicInteger(0);
        handledFailure = new AtomicReference<>();
        coordinator = new IndexWalCoordinator<>(logger, buildConf(), walRuntime,
                new IndexRetryPolicy(1, 10),
                drainCalls::incrementAndGet, flushCalls::incrementAndGet,
                () -> SegmentIndexState.READY, handledFailure::set,
                lastAppliedWalLsn);
    }

    @Test
    void recover_updatesLastAppliedLsnFromRecoveryMax() {
        when(walRuntime.isEnabled()).thenReturn(true);
        when(walRuntime.recover(any()))
                .thenReturn(new WalRuntime.RecoveryResult(3L, 7L, false));
        lastAppliedWalLsn.set(5L);

        coordinator.recover(replayRecord -> {
        });

        assertEquals(7L, lastAppliedWalLsn.get());
        verify(walRuntime).recover(any());
    }

    @Test
    void appendPut_forcesCheckpointUntilRetentionPressureClears() {
        when(walRuntime.isEnabled()).thenReturn(true);
        when(walRuntime.isRetentionPressure()).thenReturn(true, true, false);
        when(walRuntime.appendPut(1, "v1")).thenReturn(11L);
        lastAppliedWalLsn.set(9L);

        final long lsn = coordinator.appendPut(1, "v1");

        assertEquals(11L, lsn);
        assertEquals(1, drainCalls.get());
        assertEquals(1, flushCalls.get());
        verify(walRuntime).onCheckpoint(9L);
        verify(walRuntime).appendPut(1, "v1");
    }

    @Test
    void appendDelete_routesSyncFailureToErrorHandler() {
        final IndexException failure = new IndexException("sync failure");
        when(walRuntime.isEnabled()).thenReturn(true);
        when(walRuntime.isRetentionPressure()).thenReturn(false);
        when(walRuntime.appendDelete(7)).thenThrow(failure);
        when(walRuntime.hasSyncFailure()).thenReturn(true);

        final IndexException thrown = assertThrows(IndexException.class,
                () -> coordinator.appendDelete(7));

        assertEquals(failure, thrown);
        assertEquals(failure, handledFailure.get());
    }

    @Test
    void checkpoint_ignoresSyncFailureWhenIndexAlreadyClosed() {
        final IndexException failure = new IndexException("sync failure");
        coordinator = new IndexWalCoordinator<>(logger, buildConf(), walRuntime,
                new IndexRetryPolicy(1, 10),
                drainCalls::incrementAndGet, flushCalls::incrementAndGet,
                () -> SegmentIndexState.CLOSED, handledFailure::set,
                lastAppliedWalLsn);
        when(walRuntime.isEnabled()).thenReturn(true);
        when(walRuntime.hasSyncFailure()).thenReturn(true);
        doThrow(failure).when(walRuntime).onCheckpoint(0L);

        assertThrows(IndexException.class, coordinator::checkpoint);

        assertNull(handledFailure.get());
        verifyNoMoreInteractions(logger);
    }

    @Test
    void recordAppliedLsn_keepsMaximumObservedValue() {
        when(walRuntime.isEnabled()).thenReturn(true);

        coordinator.recordAppliedLsn(4L);
        coordinator.recordAppliedLsn(2L);
        coordinator.recordAppliedLsn(9L);

        assertEquals(9L, lastAppliedWalLsn.get());
        verify(walRuntime, times(3)).isEnabled();
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName("wal-coordinator-test")
                .withContextLoggingEnabled(false)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInActivePartition(5)
                .withMaxNumberOfImmutableRunsPerPartition(2)
                .withMaxNumberOfKeysInPartitionBuffer(7)
                .withMaxNumberOfKeysInIndexBuffer(9)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withMaxNumberOfKeysInSegment(100)
                .withMaxNumberOfKeysInPartitionBeforeSplit(50)
                .withMaxNumberOfSegmentsInCache(3)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSizeInBytes(1024)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .withWal(Wal.builder()
                        .withMaxBytesBeforeForcedCheckpoint(1024L)
                        .build())
                .build();
    }
}
