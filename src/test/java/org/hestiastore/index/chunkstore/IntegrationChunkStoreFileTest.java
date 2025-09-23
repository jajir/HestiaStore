package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.hestiastore.index.TestData;
import org.hestiastore.index.datablockfile.CellPosition;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IntegrationChunkStoreFileTest {

    private static final DataBlockSize DATABLOCK_SIZE = DataBlockSize
            .ofDataBlockSize(1024);

    private static final int VERSION = 3;

    private static final String FILE_NAME = "chunkpairfilewriter-test";

    private Directory directory;

    private ChunkStoreFile chunkStoreFile;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        chunkStoreFile = new ChunkStoreFile(directory, FILE_NAME,
                DATABLOCK_SIZE);
    }

    @AfterEach
    void tearDown() {
        directory = null;
        chunkStoreFile = null;
    }

    @Test
    void test_write_and_read_one_chunk_one_block() {
        // verify write
        ChunkStoreWriterTx writerTx = chunkStoreFile.openWriteTx();
        CellPosition position;
        try (ChunkStoreWriter writer = writerTx.openWriter()) {
            position = writer.write(TestData.CHUNK_PAYLOAD_154, VERSION);
        }
        writerTx.commit();
        assertEquals(0, position.getValue());

        // Verify read
        verifyReadChunk(position, TestData.CHUNK_PAYLOAD_154);
    }

    @Test
    void test_write_and_read_three_chunks_three_blocks() {
        // verify write
        ChunkStoreWriterTx writerTx = chunkStoreFile.openWriteTx();
        final CellPosition[] positions = new CellPosition[3];
        try (ChunkStoreWriter writer = writerTx.openWriter()) {
            positions[0] = writer.write(TestData.CHUNK_PAYLOAD_1008, VERSION);
            positions[1] = writer.write(TestData.CHUNK_PAYLOAD_1008, VERSION);
            positions[2] = writer.write(TestData.CHUNK_PAYLOAD_1008, VERSION);
        }
        writerTx.commit();
        assertEquals(0, positions[0].getValue());
        assertEquals(1040, positions[1].getValue());
        assertEquals(2080, positions[2].getValue());

        // Verify read
        verifyReadChunk(positions[0], TestData.CHUNK_PAYLOAD_1008);
        verifyReadChunk(positions[1], TestData.CHUNK_PAYLOAD_1008);
        verifyReadChunk(positions[2], TestData.CHUNK_PAYLOAD_1008);
    }

    private final void verifyReadChunk(final CellPosition position,
            final ChunkPayload expectedPayload) {
        try (ChunkStoreReader reader = chunkStoreFile.openReader(position)) {
            Chunk chunk = reader.read();
            assertNotNull(chunk);
            ChunkPayload payload = chunk.getPayload();
            assertNotNull(payload);
            assertEquals(expectedPayload, payload);
        }
    }

}
