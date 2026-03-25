package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.junit.jupiter.api.Test;

class SegmentConfTest {

    @Test
    void copyConstructorPreservesValues() {
        final SegmentConf original = SegmentConf.builder()
                .withMaxNumberOfKeysInSegmentWriteCache(5)
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(6)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInChunk(2)
                .withMaxNumberOfDeltaCacheFiles(4)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSize(1024)
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()))
                .build();

        final SegmentConf copy = new SegmentConf(original);

        assertEquals(original.getMaxNumberOfKeysInSegmentWriteCache(),
                copy.getMaxNumberOfKeysInSegmentWriteCache());
        assertEquals(original.getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(),
                copy.getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance());
        assertEquals(original.getMaxNumberOfKeysInSegmentCache(),
                copy.getMaxNumberOfKeysInSegmentCache());
        assertEquals(original.getMaxNumberOfKeysInChunk(),
                copy.getMaxNumberOfKeysInChunk());
        assertEquals(original.getMaxNumberOfDeltaCacheFiles(),
                copy.getMaxNumberOfDeltaCacheFiles());
        assertEquals(original.getBloomFilterNumberOfHashFunctions(),
                copy.getBloomFilterNumberOfHashFunctions());
        assertEquals(original.getBloomFilterIndexSizeInBytes(),
                copy.getBloomFilterIndexSizeInBytes());
        assertEquals(original.getBloomFilterProbabilityOfFalsePositive(),
                copy.getBloomFilterProbabilityOfFalsePositive());
        assertEquals(original.getDiskIoBufferSize(),
                copy.getDiskIoBufferSize());
    }

    @Test
    void filterListsAreImmutable() {
        final ChunkFilterDoNothing encodingFilterToAdd = new ChunkFilterDoNothing();
        final ChunkFilterDoNothing decodingFilterToAdd = new ChunkFilterDoNothing();
        final SegmentConf conf = SegmentConf.builder()
                .withMaxNumberOfKeysInSegmentWriteCache(5)
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(6)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInChunk(2)
                .withMaxNumberOfDeltaCacheFiles(4)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSize(1024)
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()))
                .build();
        final List<ChunkFilter> encodingFilters = conf
                .getEncodingChunkFilters();
        final List<ChunkFilter> decodingFilters = conf
                .getDecodingChunkFilters();

        assertThrows(UnsupportedOperationException.class,
                () -> encodingFilters.add(encodingFilterToAdd));
        assertThrows(UnsupportedOperationException.class,
                () -> decodingFilters.add(decodingFilterToAdd));
    }

    @Test
    void supplierBackedFiltersMaterializeFreshInstances() {
        final AtomicInteger sequence = new AtomicInteger();
        final SegmentConf conf = SegmentConf.builder()
                .withMaxNumberOfKeysInSegmentWriteCache(5)
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(6)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInChunk(2)
                .withMaxNumberOfDeltaCacheFiles(4)
                .withDiskIoBufferSize(1024)
                .withEncodingChunkFilterSuppliers(List.of(
                        () -> new TrackingChunkFilter(sequence.incrementAndGet())))
                .withDecodingChunkFilterSuppliers(List.of(
                        () -> new TrackingChunkFilter(sequence.incrementAndGet())))
                .build();

        final TrackingChunkFilter first = (TrackingChunkFilter) conf
                .getEncodingChunkFilters().get(0);
        final TrackingChunkFilter second = (TrackingChunkFilter) conf
                .getEncodingChunkFilters().get(0);

        assertEquals(1, first.getId());
        assertEquals(2, second.getId());
        assertNotSame(first, second);
    }

    @Test
    void supplierGetterListsAreImmutable() {
        final SegmentConf conf = SegmentConf.builder()
                .withMaxNumberOfKeysInSegmentWriteCache(5)
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(6)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInChunk(2)
                .withMaxNumberOfDeltaCacheFiles(4)
                .withDiskIoBufferSize(1024)
                .withEncodingChunkFilterSuppliers(
                        List.of((Supplier<? extends ChunkFilter>) ChunkFilterDoNothing::new))
                .withDecodingChunkFilterSuppliers(
                        List.of((Supplier<? extends ChunkFilter>) ChunkFilterDoNothing::new))
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> conf.getEncodingChunkFilterSuppliers()
                        .add(ChunkFilterDoNothing::new));
        assertThrows(UnsupportedOperationException.class,
                () -> conf.getDecodingChunkFilterSuppliers()
                        .add(ChunkFilterDoNothing::new));
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
        public org.hestiastore.index.chunkstore.ChunkData apply(
                final org.hestiastore.index.chunkstore.ChunkData input) {
            return input;
        }
    }
}
