package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
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
    void emptyStateRejectsRequireOpenedAndReusesSingleton() {
        final SegmentIndexLifecycleResources<Integer, String> first =
                SegmentIndexLifecycleResources.empty();
        final SegmentIndexLifecycleResources<Integer, String> second =
                SegmentIndexLifecycleResources.empty();

        assertSame(first, second);
        assertThrows(IllegalStateException.class, first::requireOpened);
    }

    @Test
    void openedStateExposesResourcesAndReturnsItselfFromRequireOpened() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> configuration = buildConf(
                "lifecycle-resources");
        final IndexRuntimeConfiguration<Integer, String> runtimeConfiguration =
                configuration.resolveRuntimeConfiguration();
        final ExecutorRegistry executorRegistry = ExecutorRegistryFixture.from(
                configuration);
        final SegmentIndexLifecycleResources<Integer, String> resources =
                SegmentIndexLifecycleResources.opened(directory, configuration,
                        runtimeConfiguration, executorRegistry);

        assertSame(directory, resources.managedDirectory());
        assertSame(configuration, resources.indexConfiguration());
        assertSame(runtimeConfiguration, resources.runtimeConfiguration());
        assertSame(executorRegistry, resources.executorRegistry());
        assertSame(resources, resources.requireOpened());
    }

    @Test
    void openedStateClosesResourcesAndReturnsEmptyState() {
        final MemDirectory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> configuration = buildConf(
                "lifecycle-resources");
        final IndexRuntimeConfiguration<Integer, String> runtimeConfiguration =
                configuration.resolveRuntimeConfiguration();
        final ExecutorRegistry executorRegistry = ExecutorRegistryFixture.from(
                configuration);
        final SegmentIndexLifecycleResources<Integer, String> resources =
                SegmentIndexLifecycleResources.opened(directory, configuration,
                        runtimeConfiguration, executorRegistry);

        final SegmentIndexLifecycleResources<Integer, String> closedResources =
                resources.close(LoggerFactory.getLogger(getClass()));

        assertSame(SegmentIndexLifecycleResources.empty(), closedResources);
        assertNull(closedResources.managedDirectory());
        assertNull(closedResources.indexConfiguration());
        assertNull(closedResources.runtimeConfiguration());
        assertNull(closedResources.executorRegistry());
    }

    private static IndexConfiguration<Integer, String> buildConf(
            final String indexName) {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name(indexName))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .maintenance(maintenance -> maintenance.segmentThreads(1))
                .maintenance(maintenance -> maintenance.registryLifecycleThreads(1))
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();
    }
}
