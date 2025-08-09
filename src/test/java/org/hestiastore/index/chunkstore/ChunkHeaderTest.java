package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class ChunkHeaderTest {

    private final static int VERSION = 1000000;
    private final static int PAYLOAD_LENGTH = 12345;
    private final static long CRC = 67890L;

    @Test
    void test_store_and_load() {
        final ChunkHeader header1 = ChunkHeader.of(Chunk.MAGIC_NUMBER, VERSION,
                PAYLOAD_LENGTH, CRC);

        final byte[] data = header1.getBytes().getData();
        final ChunkHeader header2 = ChunkHeader.of(data);

        assertEquals(Chunk.MAGIC_NUMBER, header2.getMagicNumber());
        assertEquals(VERSION, header2.getVersion());
        assertEquals(PAYLOAD_LENGTH, header2.getPayloadLength());
        assertEquals(CRC, header2.getCrc());
    }

    @Test
    void test_of_invalid_byte_length() {
        final byte[] data = new byte[Chunk.HEADER_SIZE + 1];

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> ChunkHeader.of(data));

        assertEquals("Invalid chunk header size '33', expected is '32'",
                e.getMessage());

    }

}
