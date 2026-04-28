package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
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
import org.junit.jupiter.api.Test;

class SegmentIndexLifecycleTest {

    @Test
    void createOpenInitializesResourcesAndCloseReleasesThem() {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndexLifecycle<Integer, String> lifecycle = new SegmentIndexLifecycle<>(
                directory, buildConf("lifecycle-create", 1));

        lifecycle.createIndex();
        assertTrue(lifecycle.isOpened());
        assertOpenedResources(lifecycle);

        lifecycle.close();
        assertFalse(lifecycle.isOpened());
    }

    @Test
    void openInitializesFullyManagedResources() {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndexLifecycle<Integer, String> lifecycle =
                new SegmentIndexLifecycle<>(directory,
                        buildConf("lifecycle-opened-resources", 1));

        lifecycle.createIndex();

        final SegmentIndexLifecycleResources<Integer, String> openedResources =
                assertOpenedResources(lifecycle);
        assertSame(directory, openedResources.managedDirectory());

        lifecycle.close();
    }

    @Test
    void openMergesStoredConfigurationWithOverrides() {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndexLifecycle<Integer, String> createLifecycle = new SegmentIndexLifecycle<>(
                directory, buildConf("lifecycle-open", 1));
        createLifecycle.createIndex();
        createLifecycle.close();

        final SegmentIndexLifecycle<Integer, String> openLifecycle = new SegmentIndexLifecycle<>(
                directory, buildConf("lifecycle-open", 2));
        openLifecycle.openExistingIndex();
        final SegmentIndexLifecycleResources<Integer, String> openedResources =
                assertOpenedResources(openLifecycle);
        assertEquals(2,
                openedResources.indexConfiguration()
                        .maintenance().registryLifecycleThreads());
        openLifecycle.close();
    }

    @Test
    void inMemoryConstructorSupportsCreateOpen() {
        final SegmentIndexLifecycle<Integer, String> lifecycle = new SegmentIndexLifecycle<>(
                buildConf("lifecycle-in-memory", 1));
        lifecycle.createIndex();
        assertOpenedResources(lifecycle);
        lifecycle.close();
    }

    @Test
    void explicitProviderRegistryIsUsedForPersistedCustomChunkFilters() {
        final MemDirectory directory = new MemDirectory();
        final ChunkFilterProviderRegistry registry = ChunkFilterProviderRegistry
                .builder().withDefaultProviders()
                .withProvider(new LifecycleChunkFilterProvider()).build();
        final SegmentIndexLifecycle<Integer, String> createLifecycle = new SegmentIndexLifecycle<>(
                directory, buildCustomFilterConf("lifecycle-provider"), registry);

        createLifecycle.createIndex();
        final SegmentIndexLifecycleResources<Integer, String> createdResources =
                assertOpenedResources(createLifecycle);
        assertInstanceOf(LifecycleChunkFilter.class,
                createdResources.runtimeConfiguration().getEncodingChunkFilters()
                        .get(0));
        createLifecycle.close();

        final SegmentIndexLifecycle<Integer, String> openLifecycle = new SegmentIndexLifecycle<>(
                directory, buildCustomFilterConf("lifecycle-provider"), registry);
        openLifecycle.openExistingIndex();
        final SegmentIndexLifecycleResources<Integer, String> openedResources =
                assertOpenedResources(openLifecycle);
        assertEquals(List.of(ChunkFilterSpec.ofProvider("lifecycle-filter")
                .withParameter("keyRef", "orders-main")),
                openedResources.indexConfiguration()
                        .filters().encodingChunkFilterSpecs());
        assertInstanceOf(LifecycleChunkFilter.class,
                openedResources.runtimeConfiguration().getDecodingChunkFilters()
                        .get(0));
        openLifecycle.close();
    }

    @Test
    void reopenPreservesChunkFilterSpecsByValue() {
        final MemDirectory directory = new MemDirectory();
        final ChunkFilterProviderRegistry registry = ChunkFilterProviderRegistry
                .builder().withDefaultProviders()
                .withProvider(new LifecycleChunkFilterProvider()).build();
        final IndexConfiguration<Integer, String> original = buildCustomFilterConf(
                "lifecycle-spec-roundtrip");
        final SegmentIndexLifecycle<Integer, String> createLifecycle = new SegmentIndexLifecycle<>(
                directory, original, registry);

        createLifecycle.createIndex();
        createLifecycle.close();

        final SegmentIndexLifecycle<Integer, String> openLifecycle = new SegmentIndexLifecycle<>(
                directory, buildCustomFilterConf("lifecycle-spec-roundtrip"),
                registry);
        openLifecycle.openExistingIndex();

        final IndexConfiguration<Integer, String> loaded = assertOpenedResources(
                openLifecycle).indexConfiguration();
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
        openLifecycle.close();
    }

    @Test
    void closeIsIdempotentAfterOpen() {
        final SegmentIndexLifecycle<Integer, String> lifecycle = new SegmentIndexLifecycle<>(
                buildConf("lifecycle-close", 1));
        lifecycle.createIndex();

        lifecycle.close();
        lifecycle.close();

        assertFalse(lifecycle.isOpened());
    }

    @Test
    void openFailureClearsPartiallyInitializedResources() {
        final SegmentIndexLifecycle<Integer, String> lifecycle =
                new SegmentIndexLifecycle<>(new MemDirectory(),
                        buildConf("lifecycle-open-failure", 1));

        assertThrows(RuntimeException.class, lifecycle::openExistingIndex);

        assertFalse(lifecycle.isOpened());
    }

    private static <K, V> SegmentIndexLifecycleResources<K, V> assertOpenedResources(
            final SegmentIndexLifecycle<K, V> lifecycle) {
        final SegmentIndexLifecycleResources<K, V> openedResources = lifecycle
                .openedResources();
        assertNotNull(openedResources.indexConfiguration());
        assertNotNull(openedResources.runtimeConfiguration());
        assertNotNull(openedResources.executorRegistry());
        assertNotNull(openedResources.managedDirectory());
        return openedResources;
    }

    private static IndexConfiguration<Integer, String> buildConf(
            final String indexName, final int registryLifecycleThreads) {
        return IndexConfiguration.<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))//
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))//
                .identity(identity -> identity.name(indexName))//
                .logging(logging -> logging.contextEnabled(false))//
                .segment(segment -> segment.cacheKeyLimit(10))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))//
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(6))//
                .segment(segment -> segment.chunkKeyLimit(2))//
                .segment(segment -> segment.maxKeys(100))//
                .segment(segment -> segment.cachedSegmentLimit(3))//
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))//
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))//
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))//
                .io(io -> io.diskBufferSizeBytes(1024))//
                .maintenance(maintenance -> maintenance.segmentThreads(1))//
                .maintenance(maintenance -> maintenance.registryLifecycleThreads(registryLifecycleThreads))//
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))//
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))//
                .build();
    }

    private static IndexConfiguration<Integer, String> buildCustomFilterConf(
            final String indexName) {
        final ChunkFilterSpec spec = ChunkFilterSpec
                .ofProvider("lifecycle-filter")
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
                .filters(filters -> filters.addEncodingFilter(LifecycleChunkFilter::new, spec))
                .filters(filters -> filters.addDecodingFilter(LifecycleChunkFilter::new, spec))
                .build();
    }

    private static final class LifecycleChunkFilterProvider
            implements ChunkFilterProvider {

        @Override
        public String getProviderId() {
            return "lifecycle-filter";
        }

        @Override
        public Supplier<? extends ChunkFilter> createEncodingSupplier(
                final ChunkFilterSpec spec) {
            return LifecycleChunkFilter::new;
        }

        @Override
        public Supplier<? extends ChunkFilter> createDecodingSupplier(
                final ChunkFilterSpec spec) {
            return LifecycleChunkFilter::new;
        }
    }

    public static final class LifecycleChunkFilter implements ChunkFilter {

        @Override
        public ChunkData apply(final ChunkData input) {
            return input;
        }
    }
}
