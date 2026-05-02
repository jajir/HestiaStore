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
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolverImpl;
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
                .identity(identity -> identity.keyTypeDescriptor(descriptor))
                .build();
        assertEquals(descriptor.getClass().getName(),
                config.identity().keyTypeDescriptor());
    }

    @Test
    void withValueTypeDescriptorFromDescriptor() {
        final TypeDescriptorLong descriptor = new TypeDescriptorLong();
        final IndexConfiguration<Integer, Long> config = IndexConfiguration
                .<Integer, Long>builder()
                .identity(identity -> identity.valueTypeDescriptor(descriptor))
                .build();
        assertEquals(descriptor.getClass().getName(),
                config.identity().valueTypeDescriptor());
    }

    @Test
    void withKeyTypeDescriptorString() {
        final String descriptor = "key-descriptor";
        final IndexConfiguration<Integer, String> config = newBuilder()
                .identity(identity -> identity.keyTypeDescriptor(descriptor))
                .build();
        assertEquals(descriptor, config.identity().keyTypeDescriptor());
    }

    @Test
    void withValueTypeDescriptorString() {
        final String descriptor = "value-descriptor";
        final IndexConfiguration<Integer, String> config = newBuilder()
                .identity(identity -> identity.valueTypeDescriptor(descriptor))
                .build();
        assertEquals(descriptor, config.identity().valueTypeDescriptor());
    }

    @Test
    void identityKeyClassSetsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .identity(identity -> identity.keyClass(Integer.class)).build();
        assertEquals(Integer.class, config.identity().keyClass());
    }

    @Test
    void identityValueClassSetsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .identity(identity -> identity.valueClass(String.class))
                .build();
        assertEquals(String.class, config.identity().valueClass());
    }

    @Test
    void identityNameSetsValue() {
        final String name = "test-index";
        final IndexConfiguration<Integer, String> config = newBuilder()
                .identity(identity -> identity.name(name)).build();
        assertEquals(name, config.identity().name());
    }

    @Test
    void segmentCacheKeyLimitSetsValue() {
        final int value = 123;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .segment(segment -> segment.cacheKeyLimit(value))
                .build();
        assertEquals(value, config.segment().cacheKeyLimit());
    }

    @Test
    void segmentChunkKeyLimitSetsValue() {
        final int value = 321;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .segment(segment -> segment.chunkKeyLimit(value)).build();
        assertEquals(value, config.segment().chunkKeyLimit());
    }

    @Test
    void segmentDeltaCacheFileLimitSetsValue() {
        final int value = 7;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .segment(segment -> segment.deltaCacheFileLimit(value)).build();
        assertEquals(value, config.segment().deltaCacheFileLimit());
    }

    @Test
    void segmentMaxKeysSetsValue() {
        final int value = 777;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .segment(segment -> segment.maxKeys(value)).build();
        assertEquals(value, config.segment().maxKeys());
    }

    @Test
    void segmentSizeAndWritePathSplitLimitsRemainIndependent() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .segment(segment -> segment.maxKeys(5))
                .writePath(writePath -> writePath
                        .segmentSplitKeyThreshold(777))
                .build();

        assertEquals(5, config.segment().maxKeys());
        assertEquals(777,
                config.writePath().segmentSplitKeyThreshold());
    }

    @Test
    void segmentCachedSegmentLimitSetsValue() {
        final int value = 42;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .segment(segment -> segment.cachedSegmentLimit(value)).build();
        assertEquals(value, config.segment().cachedSegmentLimit());
    }

    @Test
    void bloomFilterHashFunctionsSetsValue() {
        final int value = 12;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(value))
                .build();
        assertEquals(value, config.bloomFilter().hashFunctions());
    }

    @Test
    void bloomFilterFalsePositiveProbabilitySetsValue() {
        final double value = 0.123d;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .bloomFilter(bloomFilter -> bloomFilter
                        .falsePositiveProbability(value))
                .build();
        assertEquals(value, config.bloomFilter().falsePositiveProbability());
    }

    @Test
    void maintenanceRegistryLifecycleThreadsSetsValue() {
        final int value = 5;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .maintenance(maintenance -> maintenance
                        .registryLifecycleThreads(value))
                .build();
        assertEquals(value, config.maintenance().registryLifecycleThreads());
    }

    @Test
    void bloomFilterIndexSizeBytesSetsValue() {
        final int value = 64;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(value))
                .build();
        assertEquals(value, config.bloomFilter().indexSizeBytes());
    }

    @Test
    void ioDiskBufferSizeBytesSetsValue() {
        final int value = 4096;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .io(io -> io.diskBufferSizeBytes(value)).build();
        assertEquals(value, config.io().diskBufferSizeBytes());
    }

    @Test
    void loggingContextEnabledSetsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .logging(logging -> logging.contextEnabled(Boolean.FALSE))
                .build();
        assertFalse(config.logging().contextEnabled());
    }

    @Test
    void addEncodingFilterAddsBuiltInInstance() {
        final ChunkFilter filter = new ChunkFilterDoNothing();
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters.addEncodingFilter(filter)).build();
        assertEquals(1,
                config.resolveRuntimeConfiguration()
                        .getEncodingChunkFilters().size());
        assertEquals(ChunkFilterDoNothing.class,
                config.resolveRuntimeConfiguration()
                        .getEncodingChunkFilters().get(0).getClass());
        assertEquals(List.of(ChunkFilterSpecs.doNothing()),
                config.filters().encodingChunkFilterSpecs());
    }

    @Test
    void addEncodingFilterByClassInstantiatesFilter() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters
                        .addEncodingFilter(ChunkFilterCrc32Writing.class))
                .build();
        assertEquals(1,
                config.resolveRuntimeConfiguration()
                        .getEncodingChunkFilters().size());
        assertEquals(ChunkFilterCrc32Writing.class,
                config.resolveRuntimeConfiguration()
                        .getEncodingChunkFilters().get(0).getClass());
    }

    @Test
    void withEncodingFilterClassesReplacesList() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters
                        .addEncodingFilter(new ChunkFilterDoNothing())
                        .encodingFilterClasses(
                                List.of(ChunkFilterCrc32Writing.class,
                                        ChunkFilterMagicNumberWriting.class)))
                .build();
        assertEquals(2,
                config.resolveRuntimeConfiguration()
                        .getEncodingChunkFilters().size());
        assertEquals(ChunkFilterCrc32Writing.class,
                config.resolveRuntimeConfiguration()
                        .getEncodingChunkFilters().get(0).getClass());
        assertEquals(ChunkFilterMagicNumberWriting.class,
                config.resolveRuntimeConfiguration()
                        .getEncodingChunkFilters().get(1).getClass());
    }

    @Test
    void withEncodingFiltersReplacesWithProvidedInstances() {
        final ChunkFilterDoNothing first = new ChunkFilterDoNothing();
        final ChunkFilterDoNothing second = new ChunkFilterDoNothing();
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters.encodingFilters(
                        List.of(first, second)))
                .build();
        assertEquals(2,
                config.resolveRuntimeConfiguration()
                        .getEncodingChunkFilters().size());
        assertEquals(ChunkFilterDoNothing.class,
                config.resolveRuntimeConfiguration()
                        .getEncodingChunkFilters().get(0).getClass());
        assertEquals(ChunkFilterDoNothing.class,
                config.resolveRuntimeConfiguration()
                        .getEncodingChunkFilters().get(1).getClass());
        assertEquals(
                List.of(ChunkFilterSpecs.doNothing(),
                        ChunkFilterSpecs.doNothing()),
                config.filters().encodingChunkFilterSpecs());
    }

    @Test
    void addEncodingFilterRejectsCustomInstanceWithoutExplicitSpec() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder()
                        .filters(filters -> filters
                                .addEncodingFilter(new NoOpFilter()))
                        .build());
        assertEquals(
                "Custom encoding chunk filter instances require explicit persisted metadata. "
                        + "Use addEncodingFilter(ChunkFilterSpec) "
                        + "or addEncodingFilter(Class<? extends ChunkFilter>) for no-arg filters.",
                exception.getMessage());
    }

    @Test
    void addDecodingFilterAddsBuiltInInstance() {
        final ChunkFilterDoNothing filter = new ChunkFilterDoNothing();
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters.addDecodingFilter(filter)).build();
        assertEquals(1,
                config.resolveRuntimeConfiguration()
                        .getDecodingChunkFilters().size());
        assertEquals(ChunkFilterDoNothing.class,
                config.resolveRuntimeConfiguration()
                        .getDecodingChunkFilters().get(0).getClass());
        assertEquals(List.of(ChunkFilterSpecs.doNothing()),
                config.filters().decodingChunkFilterSpecs());
    }

    @Test
    void addDecodingFilterByClassInstantiatesFilter() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters
                        .addDecodingFilter(ChunkFilterCrc32Writing.class))
                .build();
        assertEquals(1,
                config.resolveRuntimeConfiguration()
                        .getDecodingChunkFilters().size());
        assertEquals(ChunkFilterCrc32Writing.class,
                config.resolveRuntimeConfiguration()
                        .getDecodingChunkFilters().get(0).getClass());
    }

    @Test
    void withDecodingFilterClassesReplacesList() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters
                        .addDecodingFilter(new ChunkFilterDoNothing())
                        .decodingFilterClasses(
                                List.of(ChunkFilterCrc32Writing.class,
                                        ChunkFilterMagicNumberWriting.class)))
                .build();
        assertEquals(2,
                config.resolveRuntimeConfiguration()
                        .getDecodingChunkFilters().size());
        assertEquals(ChunkFilterCrc32Writing.class,
                config.resolveRuntimeConfiguration()
                        .getDecodingChunkFilters().get(0).getClass());
        assertEquals(ChunkFilterMagicNumberWriting.class,
                config.resolveRuntimeConfiguration()
                        .getDecodingChunkFilters().get(1).getClass());
    }

    @Test
    void withDecodingFiltersReplacesWithProvidedInstances() {
        final ChunkFilterDoNothing first = new ChunkFilterDoNothing();
        final ChunkFilterDoNothing second = new ChunkFilterDoNothing();
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters.decodingFilters(
                        List.of(first, second)))
                .build();
        assertEquals(2,
                config.resolveRuntimeConfiguration()
                        .getDecodingChunkFilters().size());
        assertEquals(ChunkFilterDoNothing.class,
                config.resolveRuntimeConfiguration()
                        .getDecodingChunkFilters().get(0).getClass());
        assertEquals(ChunkFilterDoNothing.class,
                config.resolveRuntimeConfiguration()
                        .getDecodingChunkFilters().get(1).getClass());
        assertEquals(
                List.of(ChunkFilterSpecs.doNothing(),
                        ChunkFilterSpecs.doNothing()),
                config.filters().decodingChunkFilterSpecs());
    }

    @Test
    void addDecodingFilterRejectsCustomInstanceWithoutExplicitSpec() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder()
                        .filters(filters -> filters
                                .addDecodingFilter(new NoOpFilter()))
                        .build());
        assertEquals(
                "Custom decoding chunk filter instances require explicit persisted metadata. "
                        + "Use addDecodingFilter(ChunkFilterSpec) "
                        + "or addDecodingFilter(Class<? extends ChunkFilter>) for no-arg filters.",
                exception.getMessage());
    }

    @Test
    void addEncodingFilterWithSpecMaterializesFreshInstancesFromRegistry() {
        final AtomicInteger sequence = new AtomicInteger();
        final ChunkFilterSpec spec = ChunkFilterSpec.ofProvider("custom")
                .withParameter("keyRef", "orders-main");
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters.addEncodingFilter(spec))
                .build();
        final ResolvedIndexConfiguration<Integer, String> runtimeConfiguration = config
                .resolveRuntimeConfiguration(ChunkFilterProviderResolverImpl
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

        assertEquals(List.of(spec),
                config.filters().encodingChunkFilterSpecs());
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
                .filters(filters -> filters
                        .addEncodingFilter(new ChunkFilterDoNothing())
                        .encodingFilterRegistrations(List.of(registration)))
                .build();

        assertEquals(List.of(registration.getSpec()),
                config.filters().encodingChunkFilterSpecs());
        assertEquals(ChunkFilterDoNothing.class,
                config.resolveRuntimeConfiguration()
                        .getEncodingChunkFilters().get(0).getClass());
    }

    @Test
    void addEncodingFilterByClassRejectsNoArgsInstantiationFailure() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters
                        .addEncodingFilter(NoDefaultConstructorFilter.class))
                .build();

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> config.resolveRuntimeConfiguration()
                        .getEncodingChunkFilters());

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
