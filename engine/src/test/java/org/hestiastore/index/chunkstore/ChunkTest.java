package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.hestiastore.index.TestData;
import org.hestiastore.index.bytes.ByteSequences;
import org.junit.jupiter.api.Test;

class ChunkTest {

    private static final int VERSION = 682;

    @Test
    void test_of_sequence() {
        final ChunkHeader chunkHeader = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, 9, TestData.CHUNK_PAYLOAD_9.calculateCrc());
        final byte[] chunkBytes = new byte[ChunkHeader.HEADER_SIZE
                + TestData.BYTES_9.length()];
        ByteSequences.copy(chunkHeader.getBytesSequence(), 0, chunkBytes, 0,
                ChunkHeader.HEADER_SIZE);
        ByteSequences.copy(TestData.BYTES_9, 0, chunkBytes,
                ChunkHeader.HEADER_SIZE, TestData.BYTES_9.length());

        final Chunk chunk = Chunk.ofSequence(ByteSequences.wrap(chunkBytes));

        assertNotNull(chunk);
        assertEquals(VERSION, chunk.getHeader().getVersion());
        assertEquals(ChunkHeader.MAGIC_NUMBER,
                chunk.getHeader().getMagicNumber());
    }

}
