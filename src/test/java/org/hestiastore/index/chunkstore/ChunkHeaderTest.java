package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import org.hestiastore.index.Bytes;
import org.junit.jupiter.api.Test;

public class ChunkHeaderTest {

    private static final int VERSION = 1000000;
    private static final int PAYLOAD_LENGTH = 12345;
    private static final long CRC = 67890L;

    @Test
    void test_store_and_load() {
        final long flags = 42L;
        final ChunkHeader header1 = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, PAYLOAD_LENGTH, CRC, flags);

        final byte[] data = header1.getBytes().getData();
        final ChunkHeader header2 = ChunkHeader.of(data);

        assertEquals(ChunkHeader.MAGIC_NUMBER, header2.getMagicNumber());
        assertEquals(VERSION, header2.getVersion());
        assertEquals(PAYLOAD_LENGTH, header2.getPayloadLength());
        assertEquals(CRC, header2.getCrc());
        assertEquals(flags, header2.getFlags());
    }

    @Test
    void test_of_invalid_byte_length() {
        final byte[] data = new byte[ChunkHeader.HEADER_SIZE + 1];

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> ChunkHeader.of(data));

        assertEquals("Invalid chunk header size '33', expected is '32'",
                e.getMessage());
    }

    @Test
    void test_of_allows_invalid_magic_number() {
        final byte[] data = new byte[ChunkHeader.HEADER_SIZE];

        final ChunkHeader header = ChunkHeader.of(data);

        assertEquals(0L, header.getMagicNumber());
    }

    @Test
    void test_optionalOf_data_is_null() {
        final Optional<ChunkHeader> optionalHeader = ChunkHeader
                .optionalOf(null);
        assertEquals(Optional.empty(), optionalHeader);
    }

    @Test
    void test_optionalOf_invalid_data_size() {
        final byte[] data = new byte[ChunkHeader.HEADER_SIZE + 1];
        final Optional<ChunkHeader> optionalHeader = ChunkHeader
                .optionalOf(Bytes.of(data));
        assertEquals(Optional.empty(), optionalHeader);
    }

    @Test
    void test_optionalOf_invalid_magic() {
        final byte[] data = new byte[ChunkHeader.HEADER_SIZE];
        final Optional<ChunkHeader> optionalHeader = ChunkHeader
                .optionalOf(Bytes.of(data));
        assertEquals(Optional.empty(), optionalHeader);
    }

    @Test
    void test_optionalOf() {
        final ChunkHeader header1 = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, PAYLOAD_LENGTH, CRC);
        final byte[] data = header1.getBytes().getData();
        final Optional<ChunkHeader> optionalHeader = ChunkHeader
                .optionalOf(Bytes.of(data));
        assertTrue(optionalHeader.isPresent());
        assertEquals(header1, optionalHeader.get());
    }

}
