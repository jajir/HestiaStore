package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;

import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.Bytes;
import org.junit.jupiter.api.Test;

class ChunkHeaderCodecTest {

    private static final long MAGIC = ChunkHeader.MAGIC_NUMBER;
    private static final int VERSION = 3;
    private static final int PAYLOAD_LENGTH = 42;
    private static final long CRC = 123_456_789L;
    private static final long FLAGS = 0x0FL;

    @Test
    void test_encodeProducesDecodableBytes() {
        final ChunkHeader header = ChunkHeader.of(MAGIC, VERSION,
                PAYLOAD_LENGTH, CRC, FLAGS);

        final ByteSequence encoded = ChunkHeaderCodec.encode(header);
        final ChunkHeader decoded = ChunkHeaderCodec.decode(encoded);

        assertEquals(header, decoded);
    }

    @Test
    void test_encodeReturnsBytesInstance() {
        final ChunkHeader header = ChunkHeader.of(MAGIC, VERSION,
                PAYLOAD_LENGTH, CRC, FLAGS);

        final ByteSequence encoded = ChunkHeaderCodec.encode(header);

        assertTrue(encoded instanceof Bytes);
        assertEquals(ChunkHeader.HEADER_SIZE, encoded.length());
        assertArrayEquals(expectedBytes(), encoded.toByteArray());
    }

    @Test
    void test_encodeNullHeaderThrows() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> ChunkHeaderCodec.encode(null));

        assertEquals("Property 'header' must not be null.",
                exception.getMessage());
    }

    private byte[] expectedBytes() {
        final ByteBuffer buffer = ByteBuffer.allocate(ChunkHeader.HEADER_SIZE);
        buffer.putLong(MAGIC);
        buffer.putInt(VERSION);
        buffer.putInt(PAYLOAD_LENGTH);
        buffer.putLong(CRC);
        buffer.putLong(FLAGS);
        return buffer.array();
    }
}
