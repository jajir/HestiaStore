package org.hestiastore.index.segmentindex.core.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.Wal;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
class WalRetentionPressureCoordinatorTest {

    @Mock
    private Logger logger;

    @Mock
    private WalRuntime<Integer, String> walRuntime;

    @Test
    void enforceIfNeededRunsForcedCheckpointUntilPressureClears() {
        final AtomicInteger prepareCalls = new AtomicInteger();
        final AtomicInteger flushCalls = new AtomicInteger();
        final AtomicInteger checkpointCalls = new AtomicInteger();
        final WalRetentionPressureCoordinator<Integer, String> coordinator =
                new WalRetentionPressureCoordinator<>(logger, buildConf(),
                        walRuntime, new IndexRetryPolicy(1, 10),
                        prepareCalls::incrementAndGet,
                        flushCalls::incrementAndGet,
                        checkpointCalls::incrementAndGet);
        when(walRuntime.isEnabled()).thenReturn(true);
        when(walRuntime.isRetentionPressure()).thenReturn(true, true, false);
        when(walRuntime.retainedBytes()).thenReturn(99L);

        coordinator.enforceIfNeeded();

        assertEquals(1, prepareCalls.get());
        assertEquals(1, flushCalls.get());
        assertEquals(1, checkpointCalls.get());
        verify(logger).warn(
                "event=wal_retention_pressure_start retainedBytes={} threshold={} action=force_checkpoint_backpressure",
                99L, 1024L);
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
                .writePath(writePath -> writePath.legacyImmutableRunLimit(2))
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
                .wal(wal -> wal.configuration(Wal.builder()
                        .withMaxBytesBeforeForcedCheckpoint(1024L)
                        .build()))
                .build();
    }
}
