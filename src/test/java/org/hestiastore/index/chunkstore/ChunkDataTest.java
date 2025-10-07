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

import org.hestiastore.index.Bytes;
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
        doReturn(header.getBytes(), (Bytes) null).when(reader)
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
        assertArrayEquals(originalPayload, chunkData.getPayload().getData());
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
        assertArrayEquals(payload, chunkData.getPayload().getData());
    }
}
