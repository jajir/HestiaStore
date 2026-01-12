package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.Test;

class SegmentRegistryExecutorTest {

    @Test
    void usesMinimumQueueCapacity() {
        final SegmentRegistry<Integer, String> registry = newRegistry(1);
        try {
            final ThreadPoolExecutor executor = maintenanceExecutor(registry);
            assertEquals(1, executor.getCorePoolSize());
            assertEquals(1, executor.getMaximumPoolSize());
            assertEquals(64, executor.getQueue().remainingCapacity());
        } finally {
            registry.close();
        }
    }

    @Test
    void scalesQueueCapacityWithThreads() {
        final SegmentRegistry<Integer, String> registry = newRegistry(2);
        try {
            final ThreadPoolExecutor executor = maintenanceExecutor(registry);
            assertEquals(2, executor.getCorePoolSize());
            assertEquals(2, executor.getMaximumPoolSize());
            assertEquals(128, executor.getQueue().remainingCapacity());
        } finally {
            registry.close();
        }
    }

    private ThreadPoolExecutor maintenanceExecutor(
            final SegmentRegistry<Integer, String> registry) {
        return (ThreadPoolExecutor) registry.getMaintenanceExecutor();
    }

    private SegmentRegistry<Integer, String> newRegistry(final int threads) {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withName("segment-registry-executor-test")//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInSegmentWriteCache(5)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringFlush(6)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInCache(10)//
                .withMaxNumberOfKeysInSegment(100)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withContextLoggingEnabled(false)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withNumberOfSegmentIndexMaintenanceThreads(threads)//
                .build();
        return new SegmentRegistry<>(AsyncDirectoryAdapter.wrap(
                new MemDirectory()), new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), conf);
    }
}
