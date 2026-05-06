package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.lang.reflect.Field;
import java.util.List;
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
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStorage;
import org.hestiastore.index.segmentindex.core.session.IndexContextLoggingAdapter;
import org.hestiastore.index.segmentindex.core.session.IndexInternalConcurrent;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexResourceClosingAdapter;
import org.junit.jupiter.api.Test;

class SegmentIndexBootstrapOperationTest {

    @Test
    void createWrapsRuntimeIndexWithContextLoggingWhenEnabled() {
        final SegmentIndex<Integer, String> index =
                SegmentIndexBootstrapOperation.create(new MemDirectory(),
                        buildConf("bootstrap-operation-logging", true),
                        null)
                        .create();

        try {
            assertInstanceOf(SegmentIndexResourceClosingAdapter.class, index);
            assertInstanceOf(IndexContextLoggingAdapter.class,
                    wrappedIndex(index));
        } finally {
            index.close();
        }
    }

    @Test
    void createWrapsConcurrentRuntimeIndexWhenContextLoggingDisabled() {
        final SegmentIndex<Integer, String> index =
                SegmentIndexBootstrapOperation.create(new MemDirectory(),
                        buildConf("bootstrap-operation-plain", false), null)
                        .create();

        try {
            assertInstanceOf(SegmentIndexResourceClosingAdapter.class, index);
            assertInstanceOf(IndexInternalConcurrent.class, wrappedIndex(index));
        } finally {
            index.close();
        }
    }

    @Test
    void openMergesStoredConfigurationWithOverrides() {
        final MemDirectory directory = new MemDirectory();
        SegmentIndexBootstrapOperation.create(directory,
                buildConf("bootstrap-operation-open", false, 1), null)
                .create()
                .close();

        final SegmentIndex<Integer, String> index =
                SegmentIndexBootstrapOperation.create(directory,
                        buildConf("bootstrap-operation-open", false, 2), null)
                        .open();

        try {
            assertEquals(2,
                    new IndexConfigurationStorage<Integer, String>(directory)
                            .load()
                            .maintenance().registryLifecycleThreads());
        } finally {
            index.close();
        }
    }

    @Test
    void explicitProviderResolverIsUsedForPersistedCustomChunkFilters() {
        final MemDirectory directory = new MemDirectory();
        final ChunkFilterProviderResolver resolver =
                ChunkFilterProviderResolverImpl.builder().withDefaultProviders()
                        .withProvider(new BootstrapChunkFilterProvider())
                        .build();
        final IndexConfiguration<Integer, String> original =
                buildCustomFilterConf("bootstrap-operation-provider");

        SegmentIndexBootstrapOperation.create(directory, original, resolver)
                .create().close();

        final SegmentIndex<Integer, String> index =
                SegmentIndexBootstrapOperation.create(directory,
                        buildCustomFilterConf("bootstrap-operation-provider"),
                        resolver)
                        .open();

        try {
            final IndexConfiguration<Integer, String> loaded =
                    new IndexConfigurationStorage<Integer, String>(directory)
                            .load();
            assertEquals(original.filters().encodingChunkFilterSpecs(),
                    loaded.filters().encodingChunkFilterSpecs());
            assertEquals(original.filters().decodingChunkFilterSpecs(),
                    loaded.filters().decodingChunkFilterSpecs());
            assertNotSame(original.filters().encodingChunkFilterSpecs(),
                    loaded.filters().encodingChunkFilterSpecs());
            assertNotSame(original.filters().decodingChunkFilterSpecs(),
                    loaded.filters().decodingChunkFilterSpecs());
            assertNotSame(original.filters().encodingChunkFilterSpecs().get(0),
                    loaded.filters().encodingChunkFilterSpecs().get(0));
            assertNotSame(original.filters().decodingChunkFilterSpecs().get(0),
                    loaded.filters().decodingChunkFilterSpecs().get(0));
        } finally {
            index.close();
        }
    }

    private static Object wrappedIndex(final SegmentIndex<Integer, String> index) {
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
        return buildConf(indexName, contextLoggingEnabled, 1);
    }

    private static IndexConfiguration<Integer, String> buildConf(
            final String indexName, final boolean contextLoggingEnabled,
            final int registryLifecycleThreads) {
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
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(false))
                .maintenance(maintenance -> maintenance.segmentThreads(1))
                .maintenance(maintenance -> maintenance.registryLifecycleThreads(registryLifecycleThreads))
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();
    }

    private static IndexConfiguration<Integer, String> buildCustomFilterConf(
            final String indexName) {
        final ChunkFilterSpec spec = ChunkFilterSpec
                .ofProvider("bootstrap-filter")
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
                .filters(filters -> filters.addEncodingFilter(spec))
                .filters(filters -> filters.addDecodingFilter(spec))
                .build();
    }

    private static final class BootstrapChunkFilterProvider
            implements ChunkFilterProvider {

        @Override
        public String getProviderId() {
            return "bootstrap-filter";
        }

        @Override
        public Supplier<? extends ChunkFilter> createEncodingSupplier(
                final ChunkFilterSpec spec) {
            return BootstrapChunkFilter::new;
        }

        @Override
        public Supplier<? extends ChunkFilter> createDecodingSupplier(
                final ChunkFilterSpec spec) {
            return BootstrapChunkFilter::new;
        }
    }

    public static final class BootstrapChunkFilter implements ChunkFilter {

        @Override
        public ChunkData apply(final ChunkData input) {
            return input;
        }
    }
}
