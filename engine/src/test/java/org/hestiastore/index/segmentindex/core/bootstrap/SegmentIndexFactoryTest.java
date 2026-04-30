package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.hestiastore.index.chunkstore.ChunkData;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterProvider;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolverImpl;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.core.session.IndexContextLoggingAdapter;
import org.hestiastore.index.segmentindex.core.session.IndexInternalConcurrent;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexResourceClosingAdapter;
import org.junit.jupiter.api.Test;

class SegmentIndexFactoryTest {

    @Test
    void tryOpenReturnsEmptyWhenConfigurationDoesNotExist() {
        final ChunkFilterProviderResolver registry = ChunkFilterProviderResolverImpl
                .defaultResolver();
        final Optional<SegmentIndex<Integer, String>> index = SegmentIndexFactory
                .tryOpen(new MemDirectory(), registry);

        assertTrue(index.isEmpty());
    }

    @Test
    void createAndTryOpenSupportExplicitChunkFilterProviderResolver() {
        final MemDirectory directory = new MemDirectory();
        final ChunkFilterProviderResolver registry = ChunkFilterProviderResolverImpl
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

    @Test
    void createUsesChunkFilterProviderResolverFromConfiguration() {
        final MemDirectory directory = new MemDirectory();
        final ChunkFilterProviderResolver resolver = ChunkFilterProviderResolverImpl
                .builder().withDefaultProviders()
                .withProvider(new FactoryChunkFilterProvider()).build();
        final IndexConfiguration<Integer, String> configuration = customConf(
                resolver);

        final SegmentIndex<Integer, String> index = SegmentIndexFactory.create(
                directory, configuration);

        assertFalse(index.wasClosed());
        index.close();
    }

    @Test
    void createWrapsRuntimeIndexWithContextLoggingWhenEnabled() {
        final SegmentIndex<Integer, String> index = SegmentIndexFactory.create(
                new MemDirectory(),
                buildConf("segment-index-factory-logging-test", true),
                ChunkFilterProviderResolverImpl.defaultResolver());

        assertInstanceOf(SegmentIndexResourceClosingAdapter.class, index);
        assertInstanceOf(IndexContextLoggingAdapter.class, wrappedIndex(index));

        index.close();
    }

    @Test
    void createWrapsConcurrentRuntimeIndexWhenContextLoggingDisabled() {
        final SegmentIndex<Integer, String> index = SegmentIndexFactory.create(
                new MemDirectory(),
                buildConf("segment-index-factory-plain-test", false),
                ChunkFilterProviderResolverImpl.defaultResolver());

        assertInstanceOf(SegmentIndexResourceClosingAdapter.class, index);
        assertInstanceOf(IndexInternalConcurrent.class, wrappedIndex(index));

        index.close();
    }

    private static IndexConfiguration<Integer, String> customConf() {
        return customConf(null);
    }

    private static IndexConfiguration<Integer, String> customConf(
            final ChunkFilterProviderResolver resolver) {
        final ChunkFilterSpec spec = ChunkFilterSpec.ofProvider("factory-filter")
                .withParameter("keyRef", "orders-main");
        final var builder = IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("segment-index-factory-test"))
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
                .filters(filters -> filters.encodingFilterRegistrations(List.of()))
                .filters(filters -> filters.decodingFilterRegistrations(List.of()))
                .filters(filters -> filters.addEncodingFilter(spec))
                .filters(filters -> filters.addDecodingFilter(spec));
        if (resolver != null) {
            builder.filters(filters -> filters
                    .chunkFilterProviderResolver(resolver));
        }
        return builder.build();
    }

    private Object wrappedIndex(final SegmentIndex<Integer, String> index) {
        try {
            final Field field = index.getClass().getDeclaredField("delegate");
            field.setAccessible(true);
            return field.get(index);
        } catch (final ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static IndexConfiguration<Integer, String> buildConf(
            final String indexName, final boolean contextLoggingEnabled) {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name(indexName))
                .logging(logging -> logging.contextEnabled(contextLoggingEnabled))
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
