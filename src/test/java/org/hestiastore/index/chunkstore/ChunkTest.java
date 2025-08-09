package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.TestData;
import org.junit.jupiter.api.Test;

public class ChunkTest {

    private static final int VERSION = 682;

    @Test
    void test_of_bytes() {
        final ChunkHeader chunkHeader = ChunkHeader.of(Chunk.MAGIC_NUMBER,
                VERSION, 9, TestData.PAYLOAD_9.calculateCrc());
        final Bytes chunkBytes = chunkHeader.getBytes().add(TestData.BYTES_9);

        final Chunk chunk = Chunk.of(chunkBytes);

        assertNotNull(chunk);
        assertEquals(VERSION, chunk.getHeader().getVersion());
        assertEquals(Chunk.MAGIC_NUMBER, chunk.getHeader().getMagicNumber());
    }

}
