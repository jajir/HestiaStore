package org.hestiastore.index.segmentindex.core.maintenance;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.junit.jupiter.api.Test;

class IndexExecutorContextDecoratorTest {

    @Test
    void decorateReturnsOriginalExecutorWhenContextLoggingIsDisabled() {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final ExecutorService decorated = new IndexExecutorContextDecorator(
                    buildConf(false)).decorate(executor);

            assertSame(executor, decorated);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void decorateWrapsExecutorWhenContextLoggingIsEnabled() {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final ExecutorService decorated = new IndexExecutorContextDecorator(
                    buildConf(true)).decorate(executor);

            assertInstanceOf(IndexNameMdcExecutorService.class, decorated);
        } finally {
            executor.shutdownNow();
        }
    }

    private static IndexConfiguration<Integer, String> buildConf(
            final boolean contextLoggingEnabled) {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName("index-executor-context-decorator-test")
                .withContextLoggingEnabled(contextLoggingEnabled)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInActivePartition(5)
                .withMaxNumberOfKeysInPartitionBuffer(6)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withMaxNumberOfKeysInSegment(100)
                .withMaxNumberOfSegmentsInCache(3)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSizeInBytes(1024)
                .withNumberOfIndexMaintenanceThreads(1)
                .withNumberOfSegmentMaintenanceThreads(1)
                .withNumberOfRegistryLifecycleThreads(1)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }
}
