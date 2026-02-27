package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Optional;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.junit.jupiter.api.Test;

class ChunkHeaderTest {

    private static final int VERSION = 1000000;
    private static final int PAYLOAD_LENGTH = 12345;
    private static final long CRC = 67890L;

    @Test
    void test_store_and_load() {
        final long flags = 42L;
        final ChunkHeader header1 = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, PAYLOAD_LENGTH, CRC, flags);

        final byte[] data = header1.getBytesSequence().toByteArrayCopy();
        final ChunkHeader header2 = ChunkHeader
                .ofSequence(ByteSequences.wrap(data));

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
                () -> ChunkHeader.ofSequence(ByteSequences.wrap(data)));

        assertEquals("Invalid chunk header size '33', expected '32'",
                e.getMessage());
    }

    @Test
    void test_of_invalid_magic_number() {
        final byte[] data = new byte[ChunkHeader.HEADER_SIZE];

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> ChunkHeader.ofSequence(ByteSequences.wrap(data)));

        assertEquals("Invalid chunk magic number '0', "
                + "expected '8388065835078349409'", e.getMessage());
    }

    @Test
    void test_optionalOfSequence_data_is_null() {
        final Optional<ChunkHeader> optionalHeader = ChunkHeader
                .optionalOfSequence(null);
        assertEquals(Optional.empty(), optionalHeader);
    }

    @Test
    void test_optionalOfSequence_invalid_data_size() {
        final byte[] data = new byte[ChunkHeader.HEADER_SIZE + 1];
        final Optional<ChunkHeader> optionalHeader = ChunkHeader
                .optionalOfSequence(ByteSequences.wrap(data));
        assertEquals(Optional.empty(), optionalHeader);
    }

    @Test
    void test_optionalOfSequence_invalid_magic() {
        final byte[] data = new byte[ChunkHeader.HEADER_SIZE];
        final Optional<ChunkHeader> optionalHeader = ChunkHeader
                .optionalOfSequence(ByteSequences.wrap(data));
        assertEquals(Optional.empty(), optionalHeader);
    }

    @Test
    void test_optionalOfSequence() {
        final ChunkHeader header1 = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, PAYLOAD_LENGTH, CRC);
        final byte[] data = header1.getBytesSequence().toByteArrayCopy();
        final Optional<ChunkHeader> optionalHeader = ChunkHeader
                .optionalOfSequence(ByteSequences.wrap(data));
        assertTrue(optionalHeader.isPresent());
        assertEquals(header1, optionalHeader.get());
    }

    @Test
    void test_ofSequence() {
        final ChunkHeader header1 = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, PAYLOAD_LENGTH, CRC);
        final ChunkHeader header2 = ChunkHeader.ofSequence(
                header1.getBytesSequence());

        assertEquals(header1, header2);
    }

    @Test
    void test_optionalOfSequence_present() {
        final ChunkHeader header1 = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, PAYLOAD_LENGTH, CRC);
        final Optional<ChunkHeader> optionalHeader = ChunkHeader
                .optionalOfSequence(header1.getBytesSequence());

        assertTrue(optionalHeader.isPresent());
        assertEquals(header1, optionalHeader.get());
    }

    @Test
    void test_getBytesSequence() {
        final ChunkHeader header = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, PAYLOAD_LENGTH, CRC);
        final ByteSequence sequence = header.getBytesSequence();

        assertEquals(ChunkHeader.HEADER_SIZE, sequence.length());
        assertEquals(header, ChunkHeader.ofSequence(sequence));
    }

    @Test
    void test_payload_length_must_not_be_negative() {
        final long magicNumber = ChunkHeader.MAGIC_NUMBER;
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> ChunkHeader.of(magicNumber, VERSION, -1, CRC));
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
        final byte[] invalidHeader = buffer.array();

        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> ChunkHeader.ofSequence(ByteSequences.wrap(invalidHeader)));
        assertEquals("Property 'payloadLength' must be greater than 0",
                e.getMessage());
    }

}
