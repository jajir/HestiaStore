package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

class ChunkFilterChainFactoryTest {

    @Test
    void fromFiltersKeepsConfiguredFilterInstances() {
        final ChunkFilter filter = new ChunkFilterDoNothing();
        final ChunkFilterChainFactory factory = ChunkFilterChainFactory
                .fromFilters(List.of(filter));

        final List<ChunkFilter> first = factory.materialize();
        final List<ChunkFilter> second = factory.materialize();

        assertSame(filter, first.get(0));
        assertSame(filter, second.get(0));
    }

    @Test
    void fromSuppliersCreatesFreshFiltersForEachMaterialization() {
        final AtomicInteger sequence = new AtomicInteger();
        final ChunkFilterChainFactory factory = ChunkFilterChainFactory
                .fromSuppliers(List.of(
                        () -> new TrackingChunkFilter(sequence.incrementAndGet())));

        final TrackingChunkFilter first = (TrackingChunkFilter) factory
                .materialize().get(0);
        final TrackingChunkFilter second = (TrackingChunkFilter) factory
                .materialize().get(0);

        assertEquals(1, first.getId());
        assertEquals(2, second.getId());
        assertNotSame(first, second);
    }

    @Test
    void getSuppliersReturnsImmutableOrderedCopy() {
        final ChunkFilterDoNothing first = new ChunkFilterDoNothing();
        final ChunkFilterDoNothing second = new ChunkFilterDoNothing();
        final ChunkFilterChainFactory factory = ChunkFilterChainFactory
                .fromFilters(List.of(first, second));

        final List<Supplier<? extends ChunkFilter>> suppliers = factory
                .getSuppliers();

        assertSame(first, suppliers.get(0).get());
        assertSame(second, suppliers.get(1).get());
        assertThrows(UnsupportedOperationException.class,
                () -> suppliers.add(ChunkFilterDoNothing::new));
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
