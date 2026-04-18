package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

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

        lifecycle.open(true);
        assertNotNull(lifecycle.getIndexConfiguration());
        assertNotNull(lifecycle.getIndexRuntimeConfiguration());
        assertNotNull(lifecycle.getManagedExecutorRegistry());
        assertNotNull(lifecycle.getManagedDirectory());

        lifecycle.close();
        assertNull(lifecycle.getIndexConfiguration());
        assertNull(lifecycle.getIndexRuntimeConfiguration());
        assertNull(lifecycle.getManagedExecutorRegistry());
        assertNull(lifecycle.getManagedDirectory());
    }

    @Test
    void openMergesStoredConfigurationWithOverrides() {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndexLifecycle<Integer, String> createLifecycle = new SegmentIndexLifecycle<>(
                directory, buildConf("lifecycle-open", 1));
        createLifecycle.open(true);
        createLifecycle.close();

        final SegmentIndexLifecycle<Integer, String> openLifecycle = new SegmentIndexLifecycle<>(
                directory, buildConf("lifecycle-open", 2));
        openLifecycle.open(false);
        assertEquals(2,
                openLifecycle.getIndexConfiguration()
                        .getNumberOfRegistryLifecycleThreads());
        openLifecycle.close();
    }

    @Test
    void inMemoryConstructorSupportsCreateOpen() {
        final SegmentIndexLifecycle<Integer, String> lifecycle = new SegmentIndexLifecycle<>(
                buildConf("lifecycle-in-memory", 1));
        lifecycle.open(true);
        assertNotNull(lifecycle.getManagedDirectory());
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

        createLifecycle.open(true);
        assertInstanceOf(LifecycleChunkFilter.class,
                createLifecycle.getIndexRuntimeConfiguration()
                        .getEncodingChunkFilters().get(0));
        createLifecycle.close();

        final SegmentIndexLifecycle<Integer, String> openLifecycle = new SegmentIndexLifecycle<>(
                directory, buildCustomFilterConf("lifecycle-provider"), registry);
        openLifecycle.open(false);
        assertEquals(List.of(ChunkFilterSpec.ofProvider("lifecycle-filter")
                .withParameter("keyRef", "orders-main")),
                openLifecycle.getIndexConfiguration()
                        .getEncodingChunkFilterSpecs());
        assertInstanceOf(LifecycleChunkFilter.class,
                openLifecycle.getIndexRuntimeConfiguration()
                        .getDecodingChunkFilters().get(0));
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

        createLifecycle.open(true);
        createLifecycle.close();

        final SegmentIndexLifecycle<Integer, String> openLifecycle = new SegmentIndexLifecycle<>(
                directory, buildCustomFilterConf("lifecycle-spec-roundtrip"),
                registry);
        openLifecycle.open(false);

        final IndexConfiguration<Integer, String> loaded = openLifecycle
                .getIndexConfiguration();
        assertEquals(original.getEncodingChunkFilterSpecs(),
                loaded.getEncodingChunkFilterSpecs());
        assertEquals(original.getDecodingChunkFilterSpecs(),
                loaded.getDecodingChunkFilterSpecs());
        assertNotSame(original.getEncodingChunkFilterSpecs(),
                loaded.getEncodingChunkFilterSpecs());
        assertNotSame(original.getDecodingChunkFilterSpecs(),
                loaded.getDecodingChunkFilterSpecs());
        assertNotSame(original.getEncodingChunkFilterSpecs().get(0),
                loaded.getEncodingChunkFilterSpecs().get(0));
        assertNotSame(original.getDecodingChunkFilterSpecs().get(0),
                loaded.getDecodingChunkFilterSpecs().get(0));
        openLifecycle.close();
    }

    @Test
    void closeIsIdempotentAfterOpen() {
        final SegmentIndexLifecycle<Integer, String> lifecycle = new SegmentIndexLifecycle<>(
                buildConf("lifecycle-close", 1));
        lifecycle.open(true);

        lifecycle.close();
        lifecycle.close();

        assertNull(lifecycle.getIndexConfiguration());
        assertNull(lifecycle.getIndexRuntimeConfiguration());
        assertNull(lifecycle.getManagedExecutorRegistry());
        assertNull(lifecycle.getManagedDirectory());
    }

    private static IndexConfiguration<Integer, String> buildConf(
            final String indexName, final int registryLifecycleThreads) {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withName(indexName)//
                .withContextLoggingEnabled(false)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInActivePartition(5)//
                .withMaxNumberOfKeysInPartitionBuffer(6)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInSegment(100)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withNumberOfSegmentMaintenanceThreads(1)//
                .withNumberOfRegistryLifecycleThreads(registryLifecycleThreads)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }

    private static IndexConfiguration<Integer, String> buildCustomFilterConf(
            final String indexName) {
        final ChunkFilterSpec spec = ChunkFilterSpec
                .ofProvider("lifecycle-filter")
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
                .addEncodingFilter(LifecycleChunkFilter::new, spec)
                .addDecodingFilter(LifecycleChunkFilter::new, spec)
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
