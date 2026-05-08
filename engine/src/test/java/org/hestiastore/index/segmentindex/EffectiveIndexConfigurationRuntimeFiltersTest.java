package org.hestiastore.index.segmentindex;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.hestiastore.index.chunkstore.ChunkData;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterProvider;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolverImpl;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.chunkstore.ChunkFilterSpecs;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.junit.jupiter.api.Test;

class EffectiveIndexConfigurationRuntimeFiltersTest {

    @Test
    void effectiveConfigurationUsesProvidedRegistryAndKeepsSpecs() {
        final AtomicInteger sequence = new AtomicInteger();
        final ChunkFilterSpec spec = ChunkFilterSpec.ofProvider("custom")
                .withParameter("keyRef", "orders-main");
        final IndexConfiguration<Integer, String> configuration = newBuilder()
                .filters(filters -> filters.addEncodingFilter(spec))
                .filters(filters -> filters.addDecodingFilter(spec))
                .build();
        final ChunkFilterProviderResolver registry = ChunkFilterProviderResolverImpl
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
                .build();

        final EffectiveIndexConfiguration<Integer, String> runtimeConfiguration = effective(configuration, registry);
        final TrackingChunkFilter first = assertInstanceOf(
                TrackingChunkFilter.class,
                runtimeConfiguration.filters().encodingChunkFilters().get(0));
        final TrackingChunkFilter second = assertInstanceOf(
                TrackingChunkFilter.class,
                runtimeConfiguration.filters().encodingChunkFilters().get(0));

        assertEquals(List.of(spec),
                runtimeConfiguration.filters().encodingChunkFilterSpecs());
        assertEquals(List.of(spec),
                runtimeConfiguration.filters().decodingChunkFilterSpecs());
        assertEquals(1,
                runtimeConfiguration.filters().encodingChunkFilterRegistrations()
                        .size());
        assertEquals(1, first.getId());
        assertEquals(2, second.getId());
        assertNotSame(first, second);
        assertThrows(UnsupportedOperationException.class,
                () -> runtimeConfiguration.filters().encodingChunkFilterSuppliers()
                        .add(ChunkFilterDoNothing::new));
    }

    @Test
    void effectiveConfigurationWithDefaultRegistryMaterializesBuiltIns() {
        final IndexConfiguration<Integer, String> configuration = newBuilder()
                .filters(filters -> filters.encodingFilterSpecs(
                        List.of(ChunkFilterSpecs.doNothing())))
                .filters(filters -> filters.decodingFilterSpecs(
                        List.of(ChunkFilterSpecs.doNothing())))
                .build();

        final EffectiveIndexConfiguration<Integer, String> runtimeConfiguration = effective(configuration);

        assertEquals(ChunkFilterDoNothing.class,
                runtimeConfiguration.filters().encodingChunkFilters().get(0)
                        .getClass());
        assertEquals(ChunkFilterDoNothing.class,
                runtimeConfiguration.filters().decodingChunkFilters().get(0)
                        .getClass());
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

    private static IndexConfigurationBuilder<Integer, String> newBuilder() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class)
                        .valueClass(String.class)
                        .keyTypeDescriptor(new TypeDescriptorInteger())
                        .valueTypeDescriptor(new TypeDescriptorShortString())
                        .name("effective-index-configuration-runtime-filters-test"));
    }
}
