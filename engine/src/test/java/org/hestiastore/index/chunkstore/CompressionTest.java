package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class CompressionTest {
    @Test
    void roundTripsSettingsAndRejectsUnknownOrInvalidCombinations() {
        assertEquals(0, Compression.fromId("none", 0).getLevel());
        assertEquals("none", Compression.none().getId());
        assertEquals("zstd", Compression.zstd(3).getId());
        assertEquals(22, Compression.fromId("zstd", 22).getLevel());
        assertThrows(IndexException.class, () -> Compression.zstd(0));
        assertThrows(IndexException.class, () -> Compression.zstd(23));
        assertThrows(IndexException.class, () -> Compression.fromId("none", 3));
        assertThrows(IndexException.class,
                () -> Compression.fromId("snappy", 3));
        assertThrows(IndexException.class, () -> Compression.fromId(null, 0));
    }
}
