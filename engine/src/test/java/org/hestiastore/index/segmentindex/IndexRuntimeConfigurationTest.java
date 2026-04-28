package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.hestiastore.index.chunkstore.ChunkData;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterProvider;
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.chunkstore.ChunkFilterSpecs;
import org.junit.jupiter.api.Test;

class IndexRuntimeConfigurationTest {

    @Test
    void resolveRuntimeConfigurationUsesProvidedRegistryAndKeepsSpecs() {
        final AtomicInteger sequence = new AtomicInteger();
        final ChunkFilterSpec spec = ChunkFilterSpec.ofProvider("custom")
                .withParameter("keyRef", "orders-main");
        final IndexConfiguration<Integer, String> configuration = IndexConfiguration
                .<Integer, String>builder()
                .filters(filters -> filters.addEncodingFilter(
                        () -> new TrackingChunkFilter(sequence.incrementAndGet()),
                        spec))
                .filters(filters -> filters.addDecodingFilter(
                        () -> new TrackingChunkFilter(sequence.incrementAndGet()),
                        spec))
                .build();
        final ChunkFilterProviderRegistry registry = ChunkFilterProviderRegistry
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

        final IndexRuntimeConfiguration<Integer, String> runtimeConfiguration = configuration
                .resolveRuntimeConfiguration(registry);
        final TrackingChunkFilter first = assertInstanceOf(
                TrackingChunkFilter.class,
                runtimeConfiguration.getEncodingChunkFilters().get(0));
        final TrackingChunkFilter second = assertInstanceOf(
                TrackingChunkFilter.class,
                runtimeConfiguration.getEncodingChunkFilters().get(0));

        assertSame(configuration, runtimeConfiguration.getConfiguration());
        assertEquals(List.of(spec),
                runtimeConfiguration.getEncodingChunkFilterSpecs());
        assertEquals(List.of(spec),
                runtimeConfiguration.getDecodingChunkFilterSpecs());
        assertEquals(1,
                runtimeConfiguration.getEncodingChunkFilterRegistrations()
                        .size());
        assertEquals(1, first.getId());
        assertEquals(2, second.getId());
        assertNotSame(first, second);
        assertThrows(UnsupportedOperationException.class,
                () -> runtimeConfiguration.getEncodingChunkFilterSuppliers()
                        .add(ChunkFilterDoNothing::new));
    }

    @Test
    void resolveRuntimeConfigurationWithDefaultRegistryMaterializesBuiltIns() {
        final IndexConfiguration<Integer, String> configuration = IndexConfiguration
                .<Integer, String>builder()
                .filters(filters -> filters.encodingFilterSpecs(
                        List.of(ChunkFilterSpecs.doNothing())))
                .filters(filters -> filters.decodingFilterSpecs(
                        List.of(ChunkFilterSpecs.doNothing())))
                .build();

        final IndexRuntimeConfiguration<Integer, String> runtimeConfiguration = configuration
                .resolveRuntimeConfiguration();

        assertEquals(ChunkFilterDoNothing.class,
                runtimeConfiguration.getEncodingChunkFilters().get(0)
                        .getClass());
        assertEquals(ChunkFilterDoNothing.class,
                runtimeConfiguration.getDecodingChunkFilters().get(0)
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
}
