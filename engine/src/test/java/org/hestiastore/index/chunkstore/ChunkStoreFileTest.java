package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class ChunkStoreFileTest {

    private static final DataBlockSize DATA_BLOCK_SIZE = DataBlockSize
            .ofDataBlockSize(1024);

    @Test
    void fromSuppliersMaterializesFreshFiltersForEachAccess() {
        final AtomicInteger sequence = new AtomicInteger();
        final ChunkStoreFile chunkStoreFile = ChunkStoreFile.fromSuppliers(
                new MemDirectory(), "chunk-store-file-test", DATA_BLOCK_SIZE,
                List.of(() -> new TrackingChunkFilter(sequence.incrementAndGet())),
                List.of(() -> new TrackingChunkFilter(sequence.incrementAndGet())));

        final TrackingChunkFilter first = (TrackingChunkFilter) chunkStoreFile
                .getEncodingChunkFilters().get(0);
        final TrackingChunkFilter second = (TrackingChunkFilter) chunkStoreFile
                .getEncodingChunkFilters().get(0);

        assertEquals(1, first.getId());
        assertEquals(2, second.getId());
        assertNotSame(first, second);
    }

    @Test
    void supplierGettersReturnImmutableLists() {
        final ChunkStoreFile chunkStoreFile = ChunkStoreFile.fromSuppliers(
                new MemDirectory(), "chunk-store-file-suppliers",
                DATA_BLOCK_SIZE, List.of(ChunkFilterDoNothing::new),
                List.of(ChunkFilterDoNothing::new));

        final List<Supplier<? extends ChunkFilter>> encodingSuppliers = chunkStoreFile
                .getEncodingChunkFilterSuppliers();
        final List<Supplier<? extends ChunkFilter>> decodingSuppliers = chunkStoreFile
                .getDecodingChunkFilterSuppliers();

        assertThrows(UnsupportedOperationException.class,
                () -> encodingSuppliers.add(ChunkFilterDoNothing::new));
        assertThrows(UnsupportedOperationException.class,
                () -> decodingSuppliers.add(ChunkFilterDoNothing::new));
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
