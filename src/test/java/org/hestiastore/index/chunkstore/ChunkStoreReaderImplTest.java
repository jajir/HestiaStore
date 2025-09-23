package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.TestData;
import org.hestiastore.index.datablockfile.DataBlockByteReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ChunkStoreReaderImplTest {

    private static final int VERSION = 682;

    private static final ChunkHeader CHUNK_HEADER_9 = ChunkHeader.of(
            Chunk.MAGIC_NUMBER, VERSION, 9,
            TestData.CHUNK_PAYLOAD_9.calculateCrc());
    private static final ChunkHeader CHUNK_HEADER_15 = ChunkHeader.of(
            Chunk.MAGIC_NUMBER, VERSION, 15,
            TestData.CHUNK_PAYLOAD_15.calculateCrc());

    @Mock
    private DataBlockByteReader dataBlockByteReader;

    private ChunkStoreReader reader;

    @BeforeEach
    void beforeEach() {
        reader = new ChunkStoreReaderImpl(dataBlockByteReader);
    }

    @AfterEach
    void afterEach() {
        reader.close();
    }

    @Test
    void test_one_chunk() {
        when(dataBlockByteReader.readExactly(32))//
                .thenReturn(CHUNK_HEADER_9.getBytes())//
                .thenReturn(Bytes.of(new byte[32]))// s
                .thenReturn(null);
        when(dataBlockByteReader.readExactly(16))
                .thenReturn(TestData.BYTES_9.paddedTo(16))//
                .thenReturn(null);

        final Chunk chunk = reader.read();
        assertNotNull(chunk);
        assertEquals(CHUNK_HEADER_9, chunk.getHeader());
        assertEquals(TestData.CHUNK_PAYLOAD_9, chunk.getPayload());
        assertEquals(TestData.BYTES_9, chunk.getPayload().getBytes());

        assertNull(reader.read());
        assertNull(reader.read());
    }

    @Test
    void test_two_chunks() {
        when(dataBlockByteReader.readExactly(32))//
                .thenReturn(CHUNK_HEADER_9.getBytes())//
                .thenReturn(CHUNK_HEADER_15.getBytes())//
                .thenReturn(null);
        when(dataBlockByteReader.readExactly(16))//
                .thenReturn(TestData.BYTES_9.paddedTo(16))//
                .thenReturn(TestData.BYTES_15.paddedTo(16))//
                .thenReturn(null);

        final Chunk chunk1 = reader.read();
        assertNotNull(chunk1);
        assertEquals(CHUNK_HEADER_9, chunk1.getHeader());
        assertEquals(TestData.CHUNK_PAYLOAD_9, chunk1.getPayload());
        assertEquals(TestData.BYTES_9, chunk1.getPayload().getBytes());

        final Chunk chunk2 = reader.read();
        assertNotNull(chunk2);
        assertEquals(CHUNK_HEADER_15, chunk2.getHeader());
        assertEquals(TestData.CHUNK_PAYLOAD_15, chunk2.getPayload());
        assertEquals(TestData.BYTES_15, chunk2.getPayload().getBytes());

        assertNull(reader.read());
        assertNull(reader.read());
    }

    @Test
    void test_empty() {
        assertNull(reader.read());
        assertNull(reader.read());
    }
}
