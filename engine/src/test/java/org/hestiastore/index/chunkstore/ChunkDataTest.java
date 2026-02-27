package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Optional;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.datablockfile.DataBlockByteReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ChunkDataTest {

    private static final int VERSION = 7;
    private static final long FLAGS = 0xA5A5A5A5A5A5A5A5L;
    private static final long CRC = 0xFFEECCBBAAL;

    @Mock
    private DataBlockByteReader reader;

    @Test
    void read_returns_empty_when_header_is_missing() {
        when(reader.readExactlySequence(ChunkHeader.HEADER_SIZE))
                .thenReturn(null);

        final Optional<ChunkData> result = ChunkData.read(reader);

        assertTrue(result.isEmpty());
        verify(reader).readExactlySequence(ChunkHeader.HEADER_SIZE);
        verifyNoMoreInteractions(reader);
    }

    @Test
    void read_returns_empty_when_header_invalid() {
        final byte[] invalidHeaderBytes = new byte[ChunkHeader.HEADER_SIZE];
        when(reader.readExactlySequence(ChunkHeader.HEADER_SIZE))
                .thenReturn(ByteSequences.wrap(invalidHeaderBytes));

        final Optional<ChunkData> result = ChunkData.read(reader);

        assertTrue(result.isEmpty());
        verify(reader).readExactlySequence(ChunkHeader.HEADER_SIZE);
        verifyNoMoreInteractions(reader);
    }

    @Test
    void read_throws_when_payload_missing() {
        final int payloadLength = 24;
        final ChunkHeader header = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, payloadLength, CRC, FLAGS);
        doReturn(header.getBytesSequence(), (ByteSequence) null).when(reader)
                .readExactlySequence(ChunkHeader.HEADER_SIZE);

        assertThrows(IllegalStateException.class, () -> ChunkData.read(reader));
    }

    @Test
    void read_reads_payload_trimmed_to_required_length() {
        final byte[] originalPayload = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        final int payloadLength = originalPayload.length;
        final ChunkHeader header = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, payloadLength, CRC, FLAGS);
        final byte[] paddedPayload = Arrays.copyOf(originalPayload, 16);

        when(reader.readExactlySequence(ChunkHeader.HEADER_SIZE))
                .thenReturn(header.getBytesSequence());
        when(reader.readExactlySequence(16))
                .thenReturn(ByteSequences.wrap(paddedPayload));

        final ChunkData chunkData = ChunkData.read(reader).orElseThrow();

        assertEquals(FLAGS, chunkData.getFlags());
        assertEquals(CRC, chunkData.getCrc());
        assertEquals(ChunkHeader.MAGIC_NUMBER, chunkData.getMagicNumber());
        assertEquals(VERSION, chunkData.getVersion());
        assertEquals(payloadLength, chunkData.getPayloadSequence().length());
        assertArrayEquals(originalPayload,
                chunkData.getPayloadSequence().toByteArrayCopy());
    }

    @Test
    void read_handles_payload_already_aligned() {
        final byte[] payload = new byte[32];
        Arrays.fill(payload, (byte) 0x7F);
        final int payloadLength = payload.length;
        final ChunkHeader header = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, payloadLength, CRC, FLAGS);

        doReturn(header.getBytesSequence(),
                ByteSequences.wrap(Arrays.copyOf(payload, payload.length)))
                        .when(reader)
                        .readExactlySequence(ChunkHeader.HEADER_SIZE);

        final ChunkData chunkData = ChunkData.read(reader).orElseThrow();

        assertEquals(payloadLength, chunkData.getPayloadSequence().length());
        assertArrayEquals(payload, chunkData.getPayloadSequence().toByteArrayCopy());
    }

    @Test
    void of_should_set_all_fields() {
        final long flags = 0x0102030405060708L;
        final long crc = 0x0A0B0C0D0E0F1011L;
        final long magic = 0x1122334455667788L;
        final int version = 42;
        final byte[] data = new byte[] { 9, 8, 7, 6 };
        final ByteSequence payload = ByteSequences.wrap(data);

        final ChunkData chunk = ChunkData.ofSequence(flags, crc, magic, version,
                payload);

        assertEquals(flags, chunk.getFlags());
        assertEquals(crc, chunk.getCrc());
        assertEquals(magic, chunk.getMagicNumber());
        assertEquals(version, chunk.getVersion());
        assertArrayEquals(data, chunk.getPayloadSequence().toByteArrayCopy());
    }

    @Test
    void ofSequence_should_throw_when_payload_null() {
        assertThrows(IllegalArgumentException.class,
                () -> ChunkData.ofSequence(FLAGS, CRC,
                        ChunkHeader.MAGIC_NUMBER,
                        VERSION, null));
    }

    @Test
    void withFlags_should_update_flags_and_keep_others() {
        final ChunkData base = ChunkData.ofSequence(FLAGS, CRC,
                ChunkHeader.MAGIC_NUMBER, VERSION,
                ByteSequences.wrap(new byte[] { 1, 2 }));
        final long newFlags = 0x00000000F0F0F0F0L;

        final ChunkData updated = base.withFlags(newFlags);

        assertEquals(newFlags, updated.getFlags());
        assertEquals(base.getCrc(), updated.getCrc());
        assertEquals(base.getMagicNumber(), updated.getMagicNumber());
        assertEquals(base.getVersion(), updated.getVersion());
        assertEquals(base.getPayloadSequence(), updated.getPayloadSequence());
    }

    @Test
    void withCrc_should_update_crc_and_keep_others() {
        final ChunkData base = ChunkData.ofSequence(FLAGS, CRC,
                ChunkHeader.MAGIC_NUMBER, VERSION,
                ByteSequences.wrap(new byte[] { 1, 2 }));
        final long newCrc = 0x123456789ABCDEFL;

        final ChunkData updated = base.withCrc(newCrc);

        assertEquals(newCrc, updated.getCrc());
        assertEquals(base.getFlags(), updated.getFlags());
        assertEquals(base.getMagicNumber(), updated.getMagicNumber());
        assertEquals(base.getVersion(), updated.getVersion());
        assertEquals(base.getPayloadSequence(), updated.getPayloadSequence());
    }

    @Test
    void withMagicNumber_should_update_magic_and_keep_others() {
        final ChunkData base = ChunkData.ofSequence(FLAGS, CRC,
                ChunkHeader.MAGIC_NUMBER, VERSION,
                ByteSequences.wrap(new byte[] { 1, 2 }));
        final long newMagic = ChunkHeader.MAGIC_NUMBER + 111;

        final ChunkData updated = base.withMagicNumber(newMagic);

        assertEquals(newMagic, updated.getMagicNumber());
        assertEquals(base.getFlags(), updated.getFlags());
        assertEquals(base.getCrc(), updated.getCrc());
        assertEquals(base.getVersion(), updated.getVersion());
        assertEquals(base.getPayloadSequence(), updated.getPayloadSequence());
    }

    @Test
    void withVersion_should_update_version_and_keep_others() {
        final ChunkData base = ChunkData.ofSequence(FLAGS, CRC,
                ChunkHeader.MAGIC_NUMBER, VERSION,
                ByteSequences.wrap(new byte[] { 1, 2 }));
        final int newVersion = VERSION + 5;

        final ChunkData updated = base.withVersion(newVersion);

        assertEquals(newVersion, updated.getVersion());
        assertEquals(base.getFlags(), updated.getFlags());
        assertEquals(base.getCrc(), updated.getCrc());
        assertEquals(base.getMagicNumber(), updated.getMagicNumber());
        assertEquals(base.getPayloadSequence(), updated.getPayloadSequence());
    }

    @Test
    void withPayloadSequence_should_update_payload_bytes_and_keep_others() {
        final ChunkData base = ChunkData.ofSequence(FLAGS, CRC,
                ChunkHeader.MAGIC_NUMBER, VERSION,
                ByteSequences.wrap(new byte[] { 1, 2 }));
        final ByteSequence newPayload = ByteSequences.wrap(new byte[] { 9, 8, 7 });

        final ChunkData updated = base.withPayloadSequence(newPayload);

        assertEquals(newPayload, updated.getPayloadSequence());
        assertEquals(base.getFlags(), updated.getFlags());
        assertEquals(base.getCrc(), updated.getCrc());
        assertEquals(base.getMagicNumber(), updated.getMagicNumber());
        assertEquals(base.getVersion(), updated.getVersion());
    }

    @Test
    void withPayloadSequence_should_throw_when_null() {
        final ChunkData base = ChunkData.ofSequence(FLAGS, CRC,
                ChunkHeader.MAGIC_NUMBER, VERSION,
                ByteSequences.wrap(new byte[] { 1, 2 }));

        assertThrows(IllegalArgumentException.class,
                () -> base.withPayloadSequence(null));
    }

    @Test
    void ofSequence_should_set_payload_sequence() {
        final ByteSequence payload = ByteSequences
                .wrap(new byte[] { 3, 2, 1 });

        final ChunkData chunkData = ChunkData.ofSequence(FLAGS, CRC,
                ChunkHeader.MAGIC_NUMBER, VERSION, payload);

        assertEquals(3, chunkData.getPayloadSequence().length());
        assertArrayEquals(new byte[] { 3, 2, 1 },
                chunkData.getPayloadSequence().toByteArrayCopy());
    }

    @Test
    void withPayloadSequence_should_update_payload_and_keep_others() {
        final ChunkData base = ChunkData.ofSequence(FLAGS, CRC,
                ChunkHeader.MAGIC_NUMBER, VERSION,
                ByteSequences.wrap(new byte[] { 1, 2 }));
        final ByteSequence payload = ByteSequences
                .wrap(new byte[] { 7, 8, 9 });

        final ChunkData updated = base.withPayloadSequence(payload);

        assertArrayEquals(new byte[] { 7, 8, 9 },
                updated.getPayloadSequence().toByteArrayCopy());
        assertEquals(base.getFlags(), updated.getFlags());
        assertEquals(base.getCrc(), updated.getCrc());
        assertEquals(base.getMagicNumber(), updated.getMagicNumber());
        assertEquals(base.getVersion(), updated.getVersion());
    }
}
