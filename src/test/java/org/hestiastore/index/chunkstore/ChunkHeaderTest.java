package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Optional;

import org.hestiastore.index.bytes.Bytes;
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

        final byte[] data = header1.getBytes().toByteArray();
        final ChunkHeader header2 = ChunkHeader.of(data);

        assertEquals(ChunkHeader.MAGIC_NUMBER, header2.getMagicNumber());
        assertEquals(VERSION, header2.getVersion());
        assertEquals(PAYLOAD_LENGTH, header2.getPayloadLength());
        assertEquals(CRC, header2.getCrc());
        assertEquals(flags, header2.getFlags());
    }

    @Test
    void test_of_invalid_header_size() {
        final byte[] data = new byte[ChunkHeader.HEADER_SIZE + 1];

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> ChunkHeader.of(data));

        assertEquals("Invalid chunk header size '33', expected '32'",
                e.getMessage());
    }

    @Test
    void test_of_invalid_magic_number() {
        final byte[] data = new byte[ChunkHeader.HEADER_SIZE];

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> ChunkHeader.of(data));

        assertEquals("Invalid chunk magic number '0', "
                + "expected '8388065835078349409'", e.getMessage());
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
        final byte[] data = header1.getBytes().toByteArray();
        final Optional<ChunkHeader> optionalHeader = ChunkHeader
                .optionalOf(Bytes.of(data));
        assertTrue(optionalHeader.isPresent());
        assertEquals(header1, optionalHeader.get());
    }

    @Test
    void test_payload_length_must_not_be_negative() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> ChunkHeader.of(ChunkHeader.MAGIC_NUMBER, VERSION, -1,
                        CRC));
        assertEquals("Property 'payloadLength' must be greater than 0",
                e.getMessage());
    }

    @Test
    void test_of_invalid_payload_size() {
        final ByteBuffer buffer = ByteBuffer.allocate(ChunkHeader.HEADER_SIZE);
        buffer.putLong(ChunkHeader.MAGIC_NUMBER);
        buffer.putInt(VERSION);
        buffer.putInt(-1);
        buffer.putLong(CRC);
        buffer.putLong(0L);

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> ChunkHeader.of(buffer.array()));
        assertEquals("Property 'payloadLength' must be greater than 0",
                e.getMessage());
    }

}
