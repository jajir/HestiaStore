package org.hestiastore.index.segmentindex.core.lifecycle;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.junit.jupiter.api.Test;

class SegmentIndexLifecycleOpenFlowTest {

    @Test
    void startCreatedLifecycleReturnsOpenedLifecycle() {
        final SegmentIndexLifecycle<Integer, String> lifecycle =
                SegmentIndexLifecycleOpenFlow.startCreatedLifecycle(
                        new MemDirectory(), buildConf("open-flow-create"),
                        ChunkFilterProviderRegistry.defaultRegistry());

        try {
            assertTrue(lifecycle.isOpened());
        } finally {
            lifecycle.close();
        }
    }

    @Test
    void startOpenedLifecycleReturnsOpenedLifecycleForExistingIndex() {
        final MemDirectory directory = new MemDirectory();
        final ChunkFilterProviderRegistry registry = ChunkFilterProviderRegistry
                .defaultRegistry();
        final SegmentIndexLifecycle<Integer, String> createdLifecycle =
                SegmentIndexLifecycleOpenFlow.startCreatedLifecycle(directory,
                        buildConf("open-flow-open"), registry);
        createdLifecycle.close();

        final SegmentIndexLifecycle<Integer, String> openedLifecycle =
                SegmentIndexLifecycleOpenFlow.startOpenedLifecycle(directory,
                        buildConf("open-flow-open"), registry);
        try {
            assertTrue(openedLifecycle.isOpened());
        } finally {
            openedLifecycle.close();
        }
        assertFalse(createdLifecycle.isOpened());
    }

    private static IndexConfiguration<Integer, String> buildConf(
            final String indexName) {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName(indexName)
                .withContextLoggingEnabled(false)
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
                .withNumberOfSegmentMaintenanceThreads(1)
                .withNumberOfRegistryLifecycleThreads(1)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }
}
