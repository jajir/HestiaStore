package org.hestiastore.index.segmentindex;

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
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.junit.jupiter.api.Test;

class IndexConfigurationTest {

    @Test
    void filterListsAreDefensiveAndImmutable() {
        final List<ChunkFilter> encoding = new ArrayList<>();
        encoding.add(new ChunkFilterDoNothing());
        final List<ChunkFilter> decoding = new ArrayList<>();
        decoding.add(new ChunkFilterDoNothing());

        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .withEncodingFilters(encoding)
                .withDecodingFilters(decoding)
                .build();

        encoding.add(new ChunkFilterDoNothing());
        decoding.add(new ChunkFilterDoNothing());

        assertEquals(1, config.getEncodingChunkFilters().size());
        assertEquals(1, config.getDecodingChunkFilters().size());
        final List<ChunkFilter> configEncoding = config
                .getEncodingChunkFilters();
        final List<ChunkFilter> configDecoding = config
                .getDecodingChunkFilters();
        final ChunkFilterDoNothing encodingFilterToAdd = new ChunkFilterDoNothing();
        final ChunkFilterDoNothing decodingFilterToAdd = new ChunkFilterDoNothing();
        assertThrows(UnsupportedOperationException.class,
                () -> configEncoding.add(encodingFilterToAdd));
        assertThrows(UnsupportedOperationException.class,
                () -> configDecoding.add(decodingFilterToAdd));
    }

    @Test
    void backgroundMaintenanceAutoEnabledDefaultsToTrue() {
        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .build();

        assertTrue(config.isBackgroundMaintenanceAutoEnabled());
        assertEquals(
                IndexConfigurationContract.DEFAULT_STABLE_SEGMENT_MAINTENANCE_THREADS,
                config.getNumberOfStableSegmentMaintenanceThreads());
    }

    @Test
    void resolveRuntimeConfigurationCreatesFreshFiltersForCustomProviders() {
        final AtomicInteger sequence = new AtomicInteger();
        final ChunkFilterSpec spec = ChunkFilterSpec.ofProvider("custom")
                .withParameter("keyRef", "orders-main");
        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .addEncodingFilter(
                        () -> new TrackingChunkFilter(sequence.incrementAndGet()),
                        spec)
                .addDecodingFilter(
                        () -> new TrackingChunkFilter(sequence.incrementAndGet()),
                        spec)
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
        final IndexRuntimeConfiguration<Integer, String> runtimeConfiguration = config
                .resolveRuntimeConfiguration(registry);

        final TrackingChunkFilter first = (TrackingChunkFilter) runtimeConfiguration
                .getEncodingChunkFilters().get(0);
        final TrackingChunkFilter second = (TrackingChunkFilter) runtimeConfiguration
                .getEncodingChunkFilters().get(0);

        assertEquals(List.of(spec), config.getEncodingChunkFilterSpecs());
        assertEquals(List.of(spec), config.getDecodingChunkFilterSpecs());
        assertEquals(1, runtimeConfiguration
                .getEncodingChunkFilterRegistrations().size());
        assertEquals(1,
                runtimeConfiguration.getEncodingChunkFilterSuppliers().size());
        assertEquals(1, first.getId());
        assertEquals(2, second.getId());
        assertNotSame(first, second);
        assertThrows(UnsupportedOperationException.class,
                () -> runtimeConfiguration.getEncodingChunkFilterRegistrations()
                        .add(null));
        assertThrows(UnsupportedOperationException.class,
                () -> runtimeConfiguration.getEncodingChunkFilterSuppliers().add(
                        ChunkFilterDoNothing::new));
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
