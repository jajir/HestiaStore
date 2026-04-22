package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;

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
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.junit.jupiter.api.Test;

class SegmentIndexLifecycleConfigurationResolverTest {

    @Test
    void loadConfigurationPersistsDefaultsForCreateIndex() {
        final SegmentIndexLifecycleConfigurationResolver<Integer, String> resolver =
                new SegmentIndexLifecycleConfigurationResolver<>(
                        new MemDirectory(),
                        buildConf("lifecycle-configuration-resolver-create"),
                        ChunkFilterProviderRegistry.defaultRegistry());

        final IndexConfiguration<Integer, String> loadedConfiguration =
                resolver.loadConfiguration(true);

        assertEquals("lifecycle-configuration-resolver-create",
                loadedConfiguration.getIndexName());
    }

    @Test
    void resolveRuntimeConfigurationUsesExplicitChunkFilterProviderRegistry() {
        final ChunkFilterProviderRegistry registry = ChunkFilterProviderRegistry
                .builder().withDefaultProviders()
                .withProvider(new ResolverChunkFilterProvider()).build();
        final SegmentIndexLifecycleConfigurationResolver<Integer, String> resolver =
                new SegmentIndexLifecycleConfigurationResolver<>(
                        new MemDirectory(),
                        buildCustomFilterConf(
                                "lifecycle-configuration-resolver-runtime"),
                        registry);

        final IndexRuntimeConfiguration<Integer, String> runtimeConfiguration =
                resolver.resolveRuntimeConfiguration(
                        buildCustomFilterConf(
                                "lifecycle-configuration-resolver-runtime"));

        assertNotNull(runtimeConfiguration);
        assertEquals(ResolverChunkFilter.class,
                runtimeConfiguration.getEncodingChunkFilters().get(0)
                        .getClass());
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
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }

    private static IndexConfiguration<Integer, String> buildCustomFilterConf(
            final String indexName) {
        final ChunkFilterSpec spec = ChunkFilterSpec
                .ofProvider("resolver-filter")
                .withParameter("keyRef", "orders-main");
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
                .withBackgroundMaintenanceAutoEnabled(false)
                .withNumberOfSegmentMaintenanceThreads(1)
                .withNumberOfRegistryLifecycleThreads(1)
                .addEncodingFilter(ResolverChunkFilter::new, spec)
                .addDecodingFilter(ResolverChunkFilter::new, spec)
                .build();
    }

    private static final class ResolverChunkFilterProvider
            implements ChunkFilterProvider {

        @Override
        public String getProviderId() {
            return "resolver-filter";
        }

        @Override
        public java.util.function.Supplier<? extends ChunkFilter> createEncodingSupplier(
                final ChunkFilterSpec spec) {
            return ResolverChunkFilter::new;
        }

        @Override
        public java.util.function.Supplier<? extends ChunkFilter> createDecodingSupplier(
                final ChunkFilterSpec spec) {
            return ResolverChunkFilter::new;
        }
    }

    public static final class ResolverChunkFilter implements ChunkFilter {

        @Override
        public ChunkData apply(final ChunkData input) {
            return input;
        }
    }
}
