package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.TestData;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.datablockfile.DataBlockByteReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ChunkStoreReaderImplTest {

    private static final int VERSION = 682;

    private static final ChunkHeader CHUNK_HEADER_9 = ChunkHeader.of(
            ChunkHeader.MAGIC_NUMBER, VERSION, 9,
            TestData.CHUNK_PAYLOAD_9.calculateCrc());
    private static final ChunkHeader CHUNK_HEADER_15 = ChunkHeader.of(
            ChunkHeader.MAGIC_NUMBER, VERSION, 15,
            TestData.CHUNK_PAYLOAD_15.calculateCrc());

    @Mock
    private DataBlockByteReader dataBlockByteReader;

    private ChunkStoreReader reader;

    @BeforeEach
    void beforeEach() {
        reader = new ChunkStoreReaderImpl(dataBlockByteReader,
                List.of(new ChunkFilterDoNothing()));
    }

    @AfterEach
    void afterEach() {
        reader.close();
    }

    @Test
    void test_one_chunk() {
        when(dataBlockByteReader.readExactlySequence(32))//
                .thenReturn(CHUNK_HEADER_9.getBytesSequence())//
                .thenReturn(ByteSequences.wrap(new byte[32]))//
                .thenReturn(null);
        when(dataBlockByteReader.readExactlySequence(16))
                .thenReturn(
                        ByteSequences.padToCell(TestData.BYTES_9, 16))//
                .thenReturn(null);

        final Chunk chunk = reader.read();
        assertNotNull(chunk);
        assertEquals(CHUNK_HEADER_9, chunk.getHeader());
        assertTrue(ByteSequences.contentEquals(
                TestData.CHUNK_PAYLOAD_9.getBytesSequence(),
                chunk.getPayloadSequence()));

        assertNull(reader.read());
        assertNull(reader.read());
    }

    @Test
    void test_two_chunks() {
        when(dataBlockByteReader.readExactlySequence(32))//
                .thenReturn(CHUNK_HEADER_9.getBytesSequence())//
                .thenReturn(CHUNK_HEADER_15.getBytesSequence())//
                .thenReturn(null);
        when(dataBlockByteReader.readExactlySequence(16))//
                .thenReturn(
                        ByteSequences.padToCell(TestData.BYTES_9, 16))//
                .thenReturn(ByteSequences.padToCell(TestData.BYTES_15, 16))//
                .thenReturn(null);

        final Chunk chunk1 = reader.read();
        assertNotNull(chunk1);
        assertEquals(CHUNK_HEADER_9, chunk1.getHeader());
        assertTrue(ByteSequences.contentEquals(
                TestData.CHUNK_PAYLOAD_9.getBytesSequence(),
                chunk1.getPayloadSequence()));

        final Chunk chunk2 = reader.read();
        assertNotNull(chunk2);
        assertEquals(CHUNK_HEADER_15, chunk2.getHeader());
        assertTrue(ByteSequences.contentEquals(
                TestData.CHUNK_PAYLOAD_15.getBytesSequence(),
                chunk2.getPayloadSequence()));

        assertNull(reader.read());
        assertNull(reader.read());
    }

    @Test
    void test_empty() {
        assertNull(reader.read());
        assertNull(reader.read());
    }

    @Test
    void test_one_chunk_payload_sequence() {
        when(dataBlockByteReader.readExactlySequence(32))//
                .thenReturn(CHUNK_HEADER_9.getBytesSequence())//
                .thenReturn(null);
        when(dataBlockByteReader.readExactlySequence(16))
                .thenReturn(
                        ByteSequences.padToCell(TestData.BYTES_9, 16))//
                .thenReturn(null);

        final ByteSequence payload = reader.readPayloadSequence();
        assertNotNull(payload);
        assertTrue(ByteSequences.contentEquals(TestData.BYTES_9, payload));
        assertNull(reader.readPayloadSequence());
    }
}
