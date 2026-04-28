package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.chunkstore.ChunkFilterSpecs;
import org.junit.jupiter.api.Test;

class IndexConfigurationContractTest {

    @Test
    void derivesCanonicalWritePathDefaultsFromSegmentCache() {
        final IndexConfigurationContract contract = new IndexConfigurationContract() {
            @Override
            public IndexSegmentConfiguration segment() {
                return new IndexSegmentConfiguration(100, 10, 9, 10, 3);
            }
        };

        final int segmentWriteCacheLimit = contract.writePath()
                .segmentWriteCacheKeyLimit();
        assertEquals(4, segmentWriteCacheLimit);

        final int expectedPartitionBufferLimit = Math.max(
                segmentWriteCacheLimit + 1,
                (int) Math.ceil(segmentWriteCacheLimit * 1.4));
        assertEquals(expectedPartitionBufferLimit,
                contract.writePath()
                        .segmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(Math.max(expectedPartitionBufferLimit,
                expectedPartitionBufferLimit
                        * contract.segment().cachedSegmentLimit()),
                contract.writePath().indexBufferedWriteKeyLimit());
    }

    @Test
    void providesDefaultChunkFilters() {
        final IndexConfigurationContract contract = new IndexConfigurationContract() {
        };

        assertEquals(Boolean.TRUE,
                contract.maintenance().backgroundAutoEnabled());
        assertEquals(
                IndexConfigurationContract.DEFAULT_SEGMENT_MAINTENANCE_THREADS,
                contract.maintenance().segmentThreads());

        final List<ChunkFilterSpec> encoding =
                contract.filters().encodingChunkFilterSpecs();
        assertEquals(List.of(ChunkFilterSpecs.crc32(),
                ChunkFilterSpecs.magicNumber()), encoding);

        final List<ChunkFilterSpec> decoding =
                contract.filters().decodingChunkFilterSpecs();
        assertEquals(List.of(ChunkFilterSpecs.magicNumber(),
                ChunkFilterSpecs.crc32()), decoding);
    }
}
