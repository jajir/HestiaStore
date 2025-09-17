package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.hestiastore.index.TestData;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IntegrationChunkStoreFileTest {

    private final static int BLOCK_SIZE = 1024;

    private final static int VERSION = 3;

    private final static String FILE_NAME = "chunkpairfilewriter-test";

    private Directory directory;

    private ChunkStoreFile chunkStoreFile;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        chunkStoreFile = new ChunkStoreFile(directory, FILE_NAME, BLOCK_SIZE);
    }

    @AfterEach
    void tearDown() {
        directory = null;
        chunkStoreFile = null;
    }

    @Test
    void test_simple() {
        // verify write
        ChunkStoreWriterTx writerTx = chunkStoreFile.openWriteTx();
        CellPosition position;
        try (ChunkStoreWriter writer = writerTx.openWriter()) {
            position = writer.write(TestData.PAYLOAD_154, VERSION);
        }
        writerTx.commit();
        assertEquals(0, position.getValue());

        // Verify read
        try (ChunkStoreReader reader = chunkStoreFile.openReader(position)) {
            Chunk chunk = reader.read();
            assertNotNull(chunk);
            ChunkPayload payload = chunk.getPayload();
            assertNotNull(payload);
            assertEquals(TestData.PAYLOAD_154, payload);
        }
    }

}
