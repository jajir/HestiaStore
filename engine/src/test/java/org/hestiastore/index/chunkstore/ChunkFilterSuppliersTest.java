package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

class ChunkFilterSuppliersTest {

    @Test
    void fromFiltersWrapsInstancesAndPreservesOrder() {
        final ChunkFilterDoNothing first = new ChunkFilterDoNothing();
        final ChunkFilterDoNothing second = new ChunkFilterDoNothing();

        final List<Supplier<? extends ChunkFilter>> suppliers = ChunkFilterSuppliers
                .fromFilters(List.of(first, second));

        assertSame(first, suppliers.get(0).get());
        assertSame(second, suppliers.get(1).get());
    }

    @Test
    void copySuppliersReturnsImmutableCopy() {
        final List<Supplier<? extends ChunkFilter>> source = new ArrayList<>();
        source.add(ChunkFilterDoNothing::new);

        final List<Supplier<? extends ChunkFilter>> copy = ChunkFilterSuppliers
                .copySuppliers(source);
        source.add(ChunkFilterMagicNumberWriting::new);

        assertEquals(1, copy.size());
        assertThrows(UnsupportedOperationException.class,
                () -> copy.add(ChunkFilterCrc32Writing::new));
    }

    @Test
    void materializeCreatesFreshInstancesFromSuppliers() {
        final AtomicInteger sequence = new AtomicInteger();
        final List<Supplier<? extends ChunkFilter>> suppliers = List.of(
                () -> new TrackingChunkFilter(sequence.incrementAndGet()));

        final TrackingChunkFilter first = (TrackingChunkFilter) ChunkFilterSuppliers
                .materialize(suppliers).get(0);
        final TrackingChunkFilter second = (TrackingChunkFilter) ChunkFilterSuppliers
                .materialize(suppliers).get(0);

        assertEquals(1, first.getId());
        assertEquals(2, second.getId());
        assertNotSame(first, second);
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
