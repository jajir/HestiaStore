package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import org.hestiastore.index.Bytes;
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

    @Test
    void test_of_invalid_magic_number() {
        final byte[] data = new byte[Chunk.HEADER_SIZE];

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> ChunkHeader.of(data));

        assertEquals(
                "Invalid chunk magic number '0', expected is '8388065835078349409'",
                e.getMessage());
    }

    @Test
    void test_optionalOf_data_is_null() {
        final Optional<ChunkHeader> optionalHeader = ChunkHeader
                .optionalOf(null);
        assertEquals(Optional.empty(), optionalHeader);
    }

    @Test
    void test_optionalOf_invalid_data_size() {
        final byte[] data = new byte[Chunk.HEADER_SIZE + 1];
        final Optional<ChunkHeader> optionalHeader = ChunkHeader
                .optionalOf(Bytes.of(data));
        assertEquals(Optional.empty(), optionalHeader);
    }

    @Test
    void test_optionalOf_invalid_magic() {
        final byte[] data = new byte[Chunk.HEADER_SIZE];
        final Optional<ChunkHeader> optionalHeader = ChunkHeader
                .optionalOf(Bytes.of(data));
        assertEquals(Optional.empty(), optionalHeader);
    }

    @Test
    void test_optionalOf() {
        final ChunkHeader header1 = ChunkHeader.of(Chunk.MAGIC_NUMBER, VERSION,
                PAYLOAD_LENGTH, CRC);
        final byte[] data = header1.getBytes().getData();
        final Optional<ChunkHeader> optionalHeader = ChunkHeader
                .optionalOf(Bytes.of(data));
        assertTrue(optionalHeader.isPresent());
        assertEquals(header1, optionalHeader.get());
    }

}
