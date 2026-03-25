package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.hestiastore.index.chunkstore.ChunkData;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterProvider;
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.junit.jupiter.api.Test;

class SegmentIndexFactoryTest {

    @Test
    void tryOpenReturnsEmptyWhenConfigurationDoesNotExist() {
        final Optional<SegmentIndex<Integer, String>> index = SegmentIndexFactory
                .tryOpen(new MemDirectory());

        assertTrue(index.isEmpty());
    }

    @Test
    void createAndTryOpenSupportExplicitChunkFilterProviderRegistry() {
        final MemDirectory directory = new MemDirectory();
        final ChunkFilterProviderRegistry registry = ChunkFilterProviderRegistry
                .builder().withDefaultProviders()
                .withProvider(new FactoryChunkFilterProvider()).build();
        final IndexConfiguration<Integer, String> configuration = customConf();

        final SegmentIndex<Integer, String> created = SegmentIndexFactory
                .create(directory, configuration, registry);
        created.close();

        final Optional<SegmentIndex<Integer, String>> reopened = SegmentIndexFactory
                .tryOpen(directory, registry);
        assertTrue(reopened.isPresent());
        assertFalse(reopened.get().wasClosed());
        reopened.get().close();
    }

    private static IndexConfiguration<Integer, String> customConf() {
        final ChunkFilterSpec spec = ChunkFilterSpec.ofProvider("factory-filter")
                .withParameter("keyRef", "orders-main");
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName("segment-index-factory-test")
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
                .withIndexWorkerThreadCount(1)
                .withBackgroundMaintenanceAutoEnabled(false)
                .withNumberOfStableSegmentMaintenanceThreads(1)
                .withNumberOfRegistryLifecycleThreads(1)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .withEncodingFilterRegistrations(List.of())
                .withDecodingFilterRegistrations(List.of())
                .addEncodingFilter(FactoryChunkFilter::new, spec)
                .addDecodingFilter(FactoryChunkFilter::new, spec)
                .build();
    }

    private static final class FactoryChunkFilterProvider
            implements ChunkFilterProvider {

        @Override
        public String getProviderId() {
            return "factory-filter";
        }

        @Override
        public Supplier<? extends ChunkFilter> createEncodingSupplier(
                final ChunkFilterSpec spec) {
            return FactoryChunkFilter::new;
        }

        @Override
        public Supplier<? extends ChunkFilter> createDecodingSupplier(
                final ChunkFilterSpec spec) {
            return FactoryChunkFilter::new;
        }
    }

    public static final class FactoryChunkFilter implements ChunkFilter {

        @Override
        public ChunkData apply(final ChunkData input) {
            return input;
        }
    }
}
