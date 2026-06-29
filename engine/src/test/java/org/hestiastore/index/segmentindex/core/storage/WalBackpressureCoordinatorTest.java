package org.hestiastore.index.segmentindex.core.storage;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.api.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WalBackpressureCoordinatorTest {

    @Mock
    private WalRuntime<Integer, String> walRuntime;

    @Test
    void enforceIfNeededRunsForcedCheckpointUntilPressureClears() {
        final WalCheckpointMaintenance checkpointMaintenance = mock(
                WalCheckpointMaintenance.class);
        final WalBackpressureCoordinator<Integer, String> coordinator =
                new WalBackpressureCoordinator<>(effective(buildConf()),
                        walRuntime, new BusyRetryPolicy(1, 10),
                        checkpointMaintenance, new AtomicLong(7L));
        when(walRuntime.isRetentionPressure()).thenReturn(true, true, false);
        when(walRuntime.retainedBytes()).thenReturn(99L);

        coordinator.enforceIfNeeded();

        final InOrder inOrder = inOrder(checkpointMaintenance, walRuntime);
        inOrder.verify(checkpointMaintenance).flushAndWait();
        inOrder.verify(walRuntime).onCheckpoint(7L);
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("wal-retention-pressure-coordinator-test"))
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
