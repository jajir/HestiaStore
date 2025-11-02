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
import org.hestiastore.index.bytes.Bytes;
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
        when(reader.readExactly(ChunkHeader.HEADER_SIZE)).thenReturn(null);

        final Optional<ChunkData> result = ChunkData.read(reader);

        assertTrue(result.isEmpty());
        verify(reader).readExactly(ChunkHeader.HEADER_SIZE);
        verifyNoMoreInteractions(reader);
    }

    @Test
    void read_returns_empty_when_header_invalid() {
        final byte[] invalidHeaderBytes = new byte[ChunkHeader.HEADER_SIZE];
        when(reader.readExactly(ChunkHeader.HEADER_SIZE))
                .thenReturn(Bytes.of(invalidHeaderBytes));

        final Optional<ChunkData> result = ChunkData.read(reader);

        assertTrue(result.isEmpty());
        verify(reader).readExactly(ChunkHeader.HEADER_SIZE);
        verifyNoMoreInteractions(reader);
    }

    @Test
    void read_throws_when_payload_missing() {
        final int payloadLength = 24;
        final ChunkHeader header = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, payloadLength, CRC, FLAGS);
        doReturn(header.getBytes(), (ByteSequence) null).when(reader)
                .readExactly(ChunkHeader.HEADER_SIZE);

        assertThrows(IllegalStateException.class, () -> ChunkData.read(reader));
    }

    @Test
    void read_reads_payload_trimmed_to_required_length() {
        final byte[] originalPayload = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        final int payloadLength = originalPayload.length;
        final ChunkHeader header = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, payloadLength, CRC, FLAGS);
        final byte[] paddedPayload = Arrays.copyOf(originalPayload, 16);

        when(reader.readExactly(ChunkHeader.HEADER_SIZE))
                .thenReturn(header.getBytes());
        when(reader.readExactly(16)).thenReturn(Bytes.of(paddedPayload));

        final ChunkData chunkData = ChunkData.read(reader).orElseThrow();

        assertEquals(FLAGS, chunkData.getFlags());
        assertEquals(CRC, chunkData.getCrc());
        assertEquals(ChunkHeader.MAGIC_NUMBER, chunkData.getMagicNumber());
        assertEquals(VERSION, chunkData.getVersion());
        assertEquals(payloadLength, chunkData.getPayload().length());
        assertArrayEquals(originalPayload,
                chunkData.getPayload().toByteArray());
    }

    @Test
    void read_handles_payload_already_aligned() {
        final byte[] payload = new byte[32];
        Arrays.fill(payload, (byte) 0x7F);
        final int payloadLength = payload.length;
        final ChunkHeader header = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, payloadLength, CRC, FLAGS);

        doReturn(header.getBytes(),
                Bytes.of(Arrays.copyOf(payload, payload.length))).when(reader)
                .readExactly(ChunkHeader.HEADER_SIZE);

        final ChunkData chunkData = ChunkData.read(reader).orElseThrow();

        assertEquals(payloadLength, chunkData.getPayload().length());
        assertArrayEquals(payload, chunkData.getPayload().toByteArray());
    }

    @Test
    void of_should_set_all_fields() {
        final long flags = 0x0102030405060708L;
        final long crc = 0x0A0B0C0D0E0F1011L;
        final long magic = 0x1122334455667788L;
        final int version = 42;
        final byte[] data = new byte[] { 9, 8, 7, 6 };
        final Bytes payload = Bytes.of(data);

        final ChunkData chunk = ChunkData.of(flags, crc, magic, version,
                payload);

        assertEquals(flags, chunk.getFlags());
        assertEquals(crc, chunk.getCrc());
        assertEquals(magic, chunk.getMagicNumber());
        assertEquals(version, chunk.getVersion());
        assertArrayEquals(data, chunk.getPayload().toByteArray());
    }

    @Test
    void of_should_throw_when_payload_null() {
        assertThrows(IllegalArgumentException.class, () -> ChunkData.of(FLAGS,
                CRC, ChunkHeader.MAGIC_NUMBER, VERSION, null));
    }

    @Test
    void withFlags_should_update_flags_and_keep_others() {
        final ChunkData base = ChunkData.of(FLAGS, CRC,
                ChunkHeader.MAGIC_NUMBER, VERSION,
                Bytes.of(new byte[] { 1, 2 }));
        final long newFlags = 0x00000000F0F0F0F0L;

        final ChunkData updated = base.withFlags(newFlags);

        assertEquals(newFlags, updated.getFlags());
        assertEquals(base.getCrc(), updated.getCrc());
        assertEquals(base.getMagicNumber(), updated.getMagicNumber());
        assertEquals(base.getVersion(), updated.getVersion());
        assertEquals(base.getPayload(), updated.getPayload());
    }

    @Test
    void withCrc_should_update_crc_and_keep_others() {
        final ChunkData base = ChunkData.of(FLAGS, CRC,
                ChunkHeader.MAGIC_NUMBER, VERSION,
                Bytes.of(new byte[] { 1, 2 }));
        final long newCrc = 0x123456789ABCDEFL;

        final ChunkData updated = base.withCrc(newCrc);

        assertEquals(newCrc, updated.getCrc());
        assertEquals(base.getFlags(), updated.getFlags());
        assertEquals(base.getMagicNumber(), updated.getMagicNumber());
        assertEquals(base.getVersion(), updated.getVersion());
        assertEquals(base.getPayload(), updated.getPayload());
    }

    @Test
    void withMagicNumber_should_update_magic_and_keep_others() {
        final ChunkData base = ChunkData.of(FLAGS, CRC,
                ChunkHeader.MAGIC_NUMBER, VERSION,
                Bytes.of(new byte[] { 1, 2 }));
        final long newMagic = ChunkHeader.MAGIC_NUMBER + 111;

        final ChunkData updated = base.withMagicNumber(newMagic);

        assertEquals(newMagic, updated.getMagicNumber());
        assertEquals(base.getFlags(), updated.getFlags());
        assertEquals(base.getCrc(), updated.getCrc());
        assertEquals(base.getVersion(), updated.getVersion());
        assertEquals(base.getPayload(), updated.getPayload());
    }

    @Test
    void withVersion_should_update_version_and_keep_others() {
        final ChunkData base = ChunkData.of(FLAGS, CRC,
                ChunkHeader.MAGIC_NUMBER, VERSION,
                Bytes.of(new byte[] { 1, 2 }));
        final int newVersion = VERSION + 5;

        final ChunkData updated = base.withVersion(newVersion);

        assertEquals(newVersion, updated.getVersion());
        assertEquals(base.getFlags(), updated.getFlags());
        assertEquals(base.getCrc(), updated.getCrc());
        assertEquals(base.getMagicNumber(), updated.getMagicNumber());
        assertEquals(base.getPayload(), updated.getPayload());
    }

    @Test
    void withPayload_should_update_payload_and_keep_others() {
        final ChunkData base = ChunkData.of(FLAGS, CRC,
                ChunkHeader.MAGIC_NUMBER, VERSION,
                Bytes.of(new byte[] { 1, 2 }));
        final Bytes newPayload = Bytes.of(new byte[] { 9, 8, 7 });

        final ChunkData updated = base.withPayload(newPayload);

        assertEquals(newPayload, updated.getPayload());
        assertEquals(base.getFlags(), updated.getFlags());
        assertEquals(base.getCrc(), updated.getCrc());
        assertEquals(base.getMagicNumber(), updated.getMagicNumber());
        assertEquals(base.getVersion(), updated.getVersion());
    }

    @Test
    void withPayload_should_throw_when_null() {
        final ChunkData base = ChunkData.of(FLAGS, CRC,
                ChunkHeader.MAGIC_NUMBER, VERSION,
                Bytes.of(new byte[] { 1, 2 }));

        assertThrows(IllegalArgumentException.class,
                () -> base.withPayload(null));
    }
}
