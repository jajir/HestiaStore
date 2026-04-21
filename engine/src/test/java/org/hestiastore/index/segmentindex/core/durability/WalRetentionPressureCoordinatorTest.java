package org.hestiastore.index.segmentindex.core.durability;

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
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName("wal-retention-pressure-coordinator-test")
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
