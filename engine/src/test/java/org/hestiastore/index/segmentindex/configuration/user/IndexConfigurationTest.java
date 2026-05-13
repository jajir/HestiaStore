package org.hestiastore.index.segmentindex.configuration.user;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
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
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationResolver;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.junit.jupiter.api.Test;

class IndexConfigurationTest {

    @Test
    void filterListsAreDefensiveAndImmutable() {
        final List<ChunkFilter> encoding = new ArrayList<>();
        encoding.add(new ChunkFilterDoNothing());
        final List<ChunkFilter> decoding = new ArrayList<>();
        decoding.add(new ChunkFilterDoNothing());

        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters.encodingFilters(encoding)
                        .decodingFilters(decoding))
                .build();

        encoding.add(new ChunkFilterDoNothing());
        decoding.add(new ChunkFilterDoNothing());

        assertEquals(1,
                effective(config)
                        .filters().encodingChunkFilters().size());
        assertEquals(1,
                effective(config)
                        .filters().decodingChunkFilters().size());
        final List<ChunkFilterSpec> configEncoding = config
                .filters().encodingChunkFilterSpecs();
        final List<ChunkFilterSpec> configDecoding = config
                .filters().decodingChunkFilterSpecs();
        final ChunkFilterSpec encodingFilterToAdd = ChunkFilterSpec
                .ofProvider("encoding-extra");
        final ChunkFilterSpec decodingFilterToAdd = ChunkFilterSpec
                .ofProvider("decoding-extra");
        assertThrows(UnsupportedOperationException.class,
                () -> configEncoding.add(encodingFilterToAdd));
        assertThrows(UnsupportedOperationException.class,
                () -> configDecoding.add(decodingFilterToAdd));
    }

    @Test
    void backgroundMaintenanceAutoEnabledDefaultsToTrue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .build();

        assertEquals(null, config.maintenance().backgroundAutoEnabled());
        assertEquals(
                IndexConfigurationContract.DEFAULT_SEGMENT_MAINTENANCE_THREADS,
                EffectiveIndexConfigurationResolver.resolveForCreate(config)
                        .maintenance().segmentThreads());
        assertTrue(EffectiveIndexConfigurationResolver.resolveForCreate(config)
                .maintenance().backgroundAutoEnabled());
    }

    @Test
    void effectiveConfigurationCreatesFreshFiltersForCustomProviders() {
        final AtomicInteger sequence = new AtomicInteger();
        final ChunkFilterSpec spec = ChunkFilterSpec.ofProvider("custom")
                .withParameter("keyRef", "orders-main");
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters.addEncodingFilter(spec)
                        .addDecodingFilter(spec))
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
        final EffectiveIndexConfiguration<Integer, String> runtimeConfiguration = effective(config, registry);

        final TrackingChunkFilter first = (TrackingChunkFilter) runtimeConfiguration
                .filters().encodingChunkFilters().get(0);
        final TrackingChunkFilter second = (TrackingChunkFilter) runtimeConfiguration
                .filters().encodingChunkFilters().get(0);

        assertEquals(List.of(spec),
                config.filters().encodingChunkFilterSpecs());
        assertEquals(List.of(spec),
                config.filters().decodingChunkFilterSpecs());
        assertEquals(1, runtimeConfiguration
                .filters().encodingChunkFilterRegistrations().size());
        assertEquals(1,
                runtimeConfiguration.filters().encodingChunkFilterSuppliers().size());
        assertEquals(1, first.getId());
        assertEquals(2, second.getId());
        assertNotSame(first, second);
        assertThrows(UnsupportedOperationException.class,
                () -> runtimeConfiguration.filters().encodingChunkFilterRegistrations()
                        .add(null));
        assertThrows(UnsupportedOperationException.class,
                () -> runtimeConfiguration.filters().encodingChunkFilterSuppliers().add(
                        ChunkFilterDoNothing::new));
    }

    @Test
    void effectiveConfigurationUsesFilterSectionResolverByDefault() {
        final AtomicInteger sequence = new AtomicInteger();
        final ChunkFilterSpec spec = ChunkFilterSpec.ofProvider("custom")
                .withParameter("keyRef", "orders-main");
        final ChunkFilterProviderResolver resolver = ChunkFilterProviderResolverImpl
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
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters.chunkFilterProviderResolver(resolver)
                        .addEncodingFilter(spec)
                        .addDecodingFilter(spec))
                .build();

        final EffectiveIndexConfiguration<Integer, String> runtimeConfiguration = effective(config);

        assertEquals(resolver,
                config.filters().getChunkFilterProviderResolver());
        assertEquals(1,
                ((TrackingChunkFilter) runtimeConfiguration
                        .filters().encodingChunkFilters().get(0)).getId());
        assertEquals(2,
                ((TrackingChunkFilter) runtimeConfiguration
                        .filters().decodingChunkFilters().get(0)).getId());
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
                        .name("index-configuration-test"));
    }
}
