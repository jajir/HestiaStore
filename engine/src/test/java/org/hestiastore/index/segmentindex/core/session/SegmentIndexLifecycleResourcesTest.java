package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.hestiastore.index.segmentindex.core.maintenance.IndexExecutorRegistry;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class SegmentIndexLifecycleResourcesTest {

    @Test
    void emptyStateReturnsNullsAndCloseIsNoOp() {
        final SegmentIndexLifecycleResources<Integer, String> resources =
                SegmentIndexLifecycleResources.empty();

        assertNull(resources.managedDirectory());
        assertNull(resources.indexConfiguration());
        assertNull(resources.runtimeConfiguration());
        assertNull(resources.executorRegistry());
        assertSame(resources,
                resources.close(LoggerFactory.getLogger(getClass())));
    }

    @Test
    void openedStateClosesResourcesAndReturnsEmptyState() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> configuration = buildConf(
                "lifecycle-resources");
        final IndexRuntimeConfiguration<Integer, String> runtimeConfiguration =
                configuration.resolveRuntimeConfiguration();
        final IndexExecutorRegistry executorRegistry = new IndexExecutorRegistry(
                configuration);
        final SegmentIndexLifecycleResources<Integer, String> resources =
                SegmentIndexLifecycleResources.opened(directory, configuration,
                        runtimeConfiguration, executorRegistry);

        final SegmentIndexLifecycleResources<Integer, String> closedResources =
                resources.close(LoggerFactory.getLogger(getClass()));

        assertNull(closedResources.managedDirectory());
        assertNull(closedResources.indexConfiguration());
        assertNull(closedResources.runtimeConfiguration());
        assertNull(closedResources.executorRegistry());
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
