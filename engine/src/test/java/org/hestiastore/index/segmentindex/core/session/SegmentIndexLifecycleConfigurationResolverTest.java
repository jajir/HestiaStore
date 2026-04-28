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
                loadedConfiguration.identity().name());
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
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();
    }

    private static IndexConfiguration<Integer, String> buildCustomFilterConf(
            final String indexName) {
        final ChunkFilterSpec spec = ChunkFilterSpec
                .ofProvider("resolver-filter")
                .withParameter("keyRef", "orders-main");
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
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(false))
                .maintenance(maintenance -> maintenance.segmentThreads(1))
                .maintenance(maintenance -> maintenance.registryLifecycleThreads(1))
                .filters(filters -> filters.addEncodingFilter(ResolverChunkFilter::new, spec))
                .filters(filters -> filters.addDecodingFilter(ResolverChunkFilter::new, spec))
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
