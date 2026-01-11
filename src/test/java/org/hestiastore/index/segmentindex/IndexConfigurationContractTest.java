package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.junit.jupiter.api.Test;

class IndexConfigurationContractTest {

    @Test
    void derivesWriteCacheDefaultsFromSegmentCache() {
        final IndexConfigurationContract contract = new IndexConfigurationContract() {
            @Override
            public int getMaxNumberOfKeysInSegmentCache() {
                return 9;
            }
        };

        final int writeCache = contract.getMaxNumberOfKeysInSegmentWriteCache();
        assertEquals(4, writeCache);

        final int expectedDuringFlush = Math.max(writeCache + 1,
                (int) Math.ceil(writeCache * 1.4));
        assertEquals(expectedDuringFlush,
                contract.getMaxNumberOfKeysInSegmentWriteCacheDuringFlush());
    }

    @Test
    void providesDefaultChunkFilters() {
        final IndexConfigurationContract contract = new IndexConfigurationContract() {
        };

        assertTrue(contract.isSegmentMaintenanceAutoEnabled());

        final List<?> encoding = contract.getEncodingChunkFilters();
        assertEquals(2, encoding.size());
        assertEquals(ChunkFilterCrc32Writing.class,
                encoding.get(0).getClass());
        assertEquals(ChunkFilterMagicNumberWriting.class,
                encoding.get(1).getClass());

        final List<?> decoding = contract.getDecodingChunkFilters();
        assertEquals(2, decoding.size());
        assertEquals(ChunkFilterMagicNumberValidation.class,
                decoding.get(0).getClass());
        assertEquals(ChunkFilterCrc32Validation.class,
                decoding.get(1).getClass());
    }
}
