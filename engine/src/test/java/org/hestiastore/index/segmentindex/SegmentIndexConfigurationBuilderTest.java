package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.hestiastore.index.chunkstore.ChunkData;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.chunkstore.ChunkFilterProvider;
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.chunkstore.ChunkFilterRegistration;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.chunkstore.ChunkFilterSpecs;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.junit.jupiter.api.Test;

class SegmentIndexConfigurationBuilderTest {

    @Test
    void withKeyTypeDescriptorFromDescriptor() {
        final TypeDescriptorInteger descriptor = new TypeDescriptorInteger();
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withKeyTypeDescriptor(descriptor).build();
        assertEquals(descriptor.getClass().getName(),
                config.getKeyTypeDescriptor());
    }

    @Test
    void withValueTypeDescriptorFromDescriptor() {
        final TypeDescriptorLong descriptor = new TypeDescriptorLong();
        final IndexConfiguration<Integer, Long> config = IndexConfiguration
                .<Integer, Long>builder()
                .withValueTypeDescriptor(descriptor).build();
        assertEquals(descriptor.getClass().getName(),
                config.getValueTypeDescriptor());
    }

    @Test
    void withKeyTypeDescriptorString() {
        final String descriptor = "key-descriptor";
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withKeyTypeDescriptor(descriptor).build();
        assertEquals(descriptor, config.getKeyTypeDescriptor());
    }

    @Test
    void withValueTypeDescriptorString() {
        final String descriptor = "value-descriptor";
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withValueTypeDescriptor(descriptor).build();
        assertEquals(descriptor, config.getValueTypeDescriptor());
    }

    @Test
    void withKeyClassSetsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withKeyClass(Integer.class).build();
        assertEquals(Integer.class, config.getKeyClass());
    }

    @Test
    void withValueClassSetsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withValueClass(String.class).build();
        assertEquals(String.class, config.getValueClass());
    }

    @Test
    void withNameSetsValue() {
        final String name = "test-index";
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withName(name).build();
        assertEquals(name, config.getIndexName());
    }

    @Test
    void withMaxNumberOfKeysInSegmentCacheSetsValue() {
        final int value = 123;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withMaxNumberOfKeysInSegmentCache(value)
                .build();
        assertEquals(value, config.getMaxNumberOfKeysInSegmentCache());
    }

    @Test
    void withMaxNumberOfKeysInSegmentChunkSetsValue() {
        final int value = 321;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withMaxNumberOfKeysInSegmentChunk(value).build();
        assertEquals(value, config.getMaxNumberOfKeysInSegmentChunk());
    }

    @Test
    void withMaxNumberOfDeltaCacheFilesSetsValue() {
        final int value = 7;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withMaxNumberOfDeltaCacheFiles(value).build();
        assertEquals(value, config.getMaxNumberOfDeltaCacheFiles());
    }

    @Test
    void withMaxNumberOfKeysInSegmentSetsValue() {
        final int value = 777;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withMaxNumberOfKeysInSegment(value).build();
        assertEquals(value, config.getMaxNumberOfKeysInSegment());
    }

    @Test
    void segmentAndPartitionSplitLimitsRemainIndependent() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withMaxNumberOfKeysInSegment(5)
                .withMaxNumberOfKeysInPartitionBeforeSplit(777)
                .build();

        assertEquals(5, config.getMaxNumberOfKeysInSegment());
        assertEquals(777, config.getMaxNumberOfKeysInPartitionBeforeSplit());
    }

    @Test
    void withMaxNumberOfSegmentsInCacheSetsValue() {
        final int value = 42;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withMaxNumberOfSegmentsInCache(value).build();
        assertEquals(value, config.getMaxNumberOfSegmentsInCache());
    }

    @Test
    void withBloomFilterNumberOfHashFunctionsSetsValue() {
        final int value = 12;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withBloomFilterNumberOfHashFunctions(value).build();
        assertEquals(value, config.getBloomFilterNumberOfHashFunctions());
    }

    @Test
    void withBloomFilterProbabilityOfFalsePositiveSetsValue() {
        final double value = 0.123d;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withBloomFilterProbabilityOfFalsePositive(value).build();
        assertEquals(value,
                config.getBloomFilterProbabilityOfFalsePositive());
    }

    @Test
    void withNumberOfRegistryLifecycleThreadsSetsValue() {
        final int value = 5;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withNumberOfRegistryLifecycleThreads(value).build();
        assertEquals(value, config.getNumberOfRegistryLifecycleThreads());
    }

    @Test
    void withBloomFilterIndexSizeInBytesSetsValue() {
        final int value = 64;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withBloomFilterIndexSizeInBytes(value).build();
        assertEquals(value, config.getBloomFilterIndexSizeInBytes());
    }

    @Test
    void withDiskIoBufferSizeInBytesSetsValue() {
        final int value = 4096;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withDiskIoBufferSizeInBytes(value).build();
        assertEquals(value, config.getDiskIoBufferSize());
    }

    @Test
    void withContextLoggingEnabledSetsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withContextLoggingEnabled(Boolean.FALSE).build();
        assertFalse(config.isContextLoggingEnabled());
    }

    @Test
    void addEncodingFilterAddsBuiltInInstance() {
        final ChunkFilter filter = new ChunkFilterDoNothing();
        final IndexConfiguration<Integer, String> config = newBuilder()
                .addEncodingFilter(filter).build();
        assertEquals(1, config.getEncodingChunkFilters().size());
        assertEquals(ChunkFilterDoNothing.class,
                config.getEncodingChunkFilters().get(0).getClass());
        assertEquals(List.of(ChunkFilterSpecs.doNothing()),
                config.getEncodingChunkFilterSpecs());
    }

    @Test
    void addEncodingFilterByClassInstantiatesFilter() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .addEncodingFilter(ChunkFilterCrc32Writing.class).build();
        assertEquals(1, config.getEncodingChunkFilters().size());
        assertEquals(ChunkFilterCrc32Writing.class,
                config.getEncodingChunkFilters().get(0).getClass());
    }

    @Test
    void withEncodingFilterClassesReplacesList() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .addEncodingFilter(new ChunkFilterDoNothing())
                .withEncodingFilterClasses(List.of(ChunkFilterCrc32Writing.class,
                        ChunkFilterMagicNumberWriting.class))
                .build();
        assertEquals(2, config.getEncodingChunkFilters().size());
        assertEquals(ChunkFilterCrc32Writing.class,
                config.getEncodingChunkFilters().get(0).getClass());
        assertEquals(ChunkFilterMagicNumberWriting.class,
                config.getEncodingChunkFilters().get(1).getClass());
    }

    @Test
    void withEncodingFiltersReplacesWithProvidedInstances() {
        final ChunkFilterDoNothing first = new ChunkFilterDoNothing();
        final ChunkFilterDoNothing second = new ChunkFilterDoNothing();
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withEncodingFilters(List.of(first, second)).build();
        assertEquals(2, config.getEncodingChunkFilters().size());
        assertEquals(ChunkFilterDoNothing.class,
                config.getEncodingChunkFilters().get(0).getClass());
        assertEquals(ChunkFilterDoNothing.class,
                config.getEncodingChunkFilters().get(1).getClass());
        assertEquals(
                List.of(ChunkFilterSpecs.doNothing(),
                        ChunkFilterSpecs.doNothing()),
                config.getEncodingChunkFilterSpecs());
    }

    @Test
    void addEncodingFilterRejectsCustomInstanceWithoutExplicitSpec() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().addEncodingFilter(new NoOpFilter()).build());
        assertEquals(
                "Custom encoding chunk filter instances require explicit persisted metadata. "
                        + "Use addEncodingFilter(Supplier<? extends ChunkFilter>, ChunkFilterSpec) "
                        + "or addEncodingFilter(Class<? extends ChunkFilter>) for no-arg filters.",
                exception.getMessage());
    }

    @Test
    void addDecodingFilterAddsBuiltInInstance() {
        final ChunkFilterDoNothing filter = new ChunkFilterDoNothing();
        final IndexConfiguration<Integer, String> config = newBuilder()
                .addDecodingFilter(filter).build();
        assertEquals(1, config.getDecodingChunkFilters().size());
        assertEquals(ChunkFilterDoNothing.class,
                config.getDecodingChunkFilters().get(0).getClass());
        assertEquals(List.of(ChunkFilterSpecs.doNothing()),
                config.getDecodingChunkFilterSpecs());
    }

    @Test
    void addDecodingFilterByClassInstantiatesFilter() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .addDecodingFilter(ChunkFilterCrc32Writing.class).build();
        assertEquals(1, config.getDecodingChunkFilters().size());
        assertEquals(ChunkFilterCrc32Writing.class,
                config.getDecodingChunkFilters().get(0).getClass());
    }

    @Test
    void withDecodingFilterClassesReplacesList() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .addDecodingFilter(new ChunkFilterDoNothing())
                .withDecodingFilterClasses(List.of(ChunkFilterCrc32Writing.class,
                        ChunkFilterMagicNumberWriting.class))
                .build();
        assertEquals(2, config.getDecodingChunkFilters().size());
        assertEquals(ChunkFilterCrc32Writing.class,
                config.getDecodingChunkFilters().get(0).getClass());
        assertEquals(ChunkFilterMagicNumberWriting.class,
                config.getDecodingChunkFilters().get(1).getClass());
    }

    @Test
    void withDecodingFiltersReplacesWithProvidedInstances() {
        final ChunkFilterDoNothing first = new ChunkFilterDoNothing();
        final ChunkFilterDoNothing second = new ChunkFilterDoNothing();
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withDecodingFilters(List.of(first, second)).build();
        assertEquals(2, config.getDecodingChunkFilters().size());
        assertEquals(ChunkFilterDoNothing.class,
                config.getDecodingChunkFilters().get(0).getClass());
        assertEquals(ChunkFilterDoNothing.class,
                config.getDecodingChunkFilters().get(1).getClass());
        assertEquals(
                List.of(ChunkFilterSpecs.doNothing(),
                        ChunkFilterSpecs.doNothing()),
                config.getDecodingChunkFilterSpecs());
    }

    @Test
    void addDecodingFilterRejectsCustomInstanceWithoutExplicitSpec() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().addDecodingFilter(new NoOpFilter()).build());
        assertEquals(
                "Custom decoding chunk filter instances require explicit persisted metadata. "
                        + "Use addDecodingFilter(Supplier<? extends ChunkFilter>, ChunkFilterSpec) "
                        + "or addDecodingFilter(Class<? extends ChunkFilter>) for no-arg filters.",
                exception.getMessage());
    }

    @Test
    void addEncodingFilterWithSupplierAndSpecMaterializesFreshInstances() {
        final AtomicInteger sequence = new AtomicInteger();
        final ChunkFilterSpec spec = ChunkFilterSpec.ofProvider("custom")
                .withParameter("keyRef", "orders-main");
        final IndexConfiguration<Integer, String> config = newBuilder()
                .addEncodingFilter(
                        () -> new TrackingChunkFilter(sequence.incrementAndGet()),
                        spec)
                .build();
        final IndexRuntimeConfiguration<Integer, String> runtimeConfiguration = config
                .resolveRuntimeConfiguration(ChunkFilterProviderRegistry
                        .builder().withDefaultProviders()
                        .withProvider(new ChunkFilterProvider() {
                            @Override
                            public String getProviderId() {
                                return "custom";
                            }

                            @Override
                            public Supplier<? extends ChunkFilter> createEncodingSupplier(
                                    final ChunkFilterSpec runtimeSpec) {
                                return () -> new TrackingChunkFilter(
                                        sequence.incrementAndGet());
                            }

                            @Override
                            public Supplier<? extends ChunkFilter> createDecodingSupplier(
                                    final ChunkFilterSpec runtimeSpec) {
                                return () -> new TrackingChunkFilter(
                                        sequence.incrementAndGet());
                            }
                        })
                        .build());

        final TrackingChunkFilter first = (TrackingChunkFilter) runtimeConfiguration
                .getEncodingChunkFilters().get(0);
        final TrackingChunkFilter second = (TrackingChunkFilter) runtimeConfiguration
                .getEncodingChunkFilters().get(0);

        assertEquals(List.of(spec), config.getEncodingChunkFilterSpecs());
        assertEquals(1, first.getId());
        assertEquals(2, second.getId());
        assertNotSame(first, second);
    }

    @Test
    void withEncodingFilterRegistrationsReplacesExistingPipeline() {
        final ChunkFilterRegistration registration = ChunkFilterRegistration.of(
                ChunkFilterSpecs.doNothing(),
                ChunkFilterDoNothing::new);
        final IndexConfiguration<Integer, String> config = newBuilder()
                .addEncodingFilter(new ChunkFilterDoNothing())
                .withEncodingFilterRegistrations(List.of(registration)).build();

        assertEquals(List.of(registration.getSpec()),
                config.getEncodingChunkFilterSpecs());
        assertEquals(ChunkFilterDoNothing.class,
                config.getEncodingChunkFilters().get(0).getClass());
    }

    @Test
    void addEncodingFilterByClassRejectsNoArgsInstantiationFailure() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .addEncodingFilter(NoDefaultConstructorFilter.class).build();

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, config::getEncodingChunkFilters);

        assertEquals(String.format("Unable to instantiate chunk filter class '%s'",
                NoDefaultConstructorFilter.class.getName()),
                exception.getMessage());
    }

    private IndexConfigurationBuilder<Integer, String> newBuilder() {
        return IndexConfiguration.<Integer, String>builder();
    }

    private static class NoOpFilter implements ChunkFilter {
        @Override
        public ChunkData apply(final ChunkData input) {
            return input;
        }
    }

    private static final class TrackingChunkFilter implements ChunkFilter {

        private final int id;

        private TrackingChunkFilter(final int id) {
            this.id = id;
        }

        private int getId() {
            return id;
        }

        @Override
        public ChunkData apply(final ChunkData input) {
            return input;
        }
    }

    private static final class NoDefaultConstructorFilter implements ChunkFilter {

        private NoDefaultConstructorFilter(final Supplier<?> ignored) {
        }

        @Override
        public ChunkData apply(final ChunkData input) {
            return input;
        }
    }
}
