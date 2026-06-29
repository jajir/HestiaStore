package org.hestiastore.index.segmentindex.core.storage;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.SegmentIndexRuntimeState;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.api.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ActiveWalCoordinatorTest {

    @Mock
    private WalRuntime<Integer, String> walRuntime;

    @Test
    void recover_updatesLastAppliedLsnFromRecoveryMax() {
        final AtomicLong lastAppliedWalLsn = new AtomicLong(5L);
        final ActiveWalCoordinator<Integer, String> coordinator =
                newCoordinator(lastAppliedWalLsn, runtimeState());
        when(walRuntime.recover(any()))
                .thenReturn(new WalRuntime.RecoveryResult(3L, 7L, false));

        coordinator.recover(replayRecord -> {
        });

        assertEquals(7L, lastAppliedWalLsn.get());
        verify(walRuntime).recover(any());
    }

    @Test
    void recordAppliedLsn_keepsMaximumObservedValue() {
        final AtomicLong lastAppliedWalLsn = new AtomicLong(0L);
        final ActiveWalCoordinator<Integer, String> coordinator =
                newCoordinator(lastAppliedWalLsn, runtimeState());

        coordinator.recordAppliedLsn(4L);
        coordinator.recordAppliedLsn(2L);
        coordinator.recordAppliedLsn(9L);

        assertEquals(9L, lastAppliedWalLsn.get());
    }

    @Test
    void checkpoint_routesRuntimeFailureThroughRuntimeState() {
        final IndexException failure = new IndexException("sync failure");
        final AtomicReference<RuntimeException> handledFailure =
                new AtomicReference<>();
        final ActiveWalCoordinator<Integer, String> coordinator =
                newCoordinator(new AtomicLong(), runtimeState(handledFailure));
        when(walRuntime.hasSyncFailure()).thenReturn(true);
        doThrow(failure).when(walRuntime).onCheckpoint(0L);

        final IndexException thrown = assertThrows(IndexException.class,
                coordinator::checkpoint);

        assertSame(failure, thrown);
        assertSame(failure, handledFailure.get());
    }

    @Test
    void checkpoint_appliesLastAppliedLsn() {
        final ActiveWalCoordinator<Integer, String> coordinator =
                newCoordinator(new AtomicLong(9L), runtimeState());

        coordinator.checkpoint();

        verify(walRuntime).onCheckpoint(9L);
    }

    private ActiveWalCoordinator<Integer, String> newCoordinator(
            final AtomicLong lastAppliedWalLsn,
            final SegmentIndexRuntimeState runtimeState) {
        return new ActiveWalCoordinator<>(effective(buildConf()),
                walRuntime, new BusyRetryPolicy(1, 10),
                mock(WalCheckpointMaintenance.class), runtimeState,
                lastAppliedWalLsn);
    }

    private SegmentIndexRuntimeState runtimeState() {
        return runtimeState(new AtomicReference<>());
    }

    private SegmentIndexRuntimeState runtimeState(
            final AtomicReference<RuntimeException> handledFailure) {
        return new SegmentIndexRuntimeState() {

            @Override
            public SegmentIndexState currentState() {
                return SegmentIndexState.READY;
            }

            @Override
            public void markRuntimeFailure(final RuntimeException failure) {
                handledFailure.set(failure);
            }
        };
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("active-index-wal-coordinator-test"))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(7))
                .writePath(writePath -> writePath.indexBufferedWriteKeyLimit(9))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .writePath(writePath -> writePath.segmentSplitKeyThreshold(50))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
                .wal(wal -> wal.configuration(IndexWalConfiguration.builder()
                        .maxBytesBeforeForcedCheckpoint(1024L)
                        .build()))
                .build();
    }
}
