package org.hestiastore.index.datablockfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.hestiastore.index.TestData;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IntegrationDataBlockFileTest {

    private final static int BLOCK_SIZE = 1024;

    private final static String FILE_NAME = "chunkpairfilewriter-test";

    private Directory directory;

    private DataBlockFile dataBlockFile;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        dataBlockFile = new DataBlockFile(directory, FILE_NAME, BLOCK_SIZE);
    }

    @AfterEach
    void tearDown() {
        directory = null;
        dataBlockFile = null;
    }

    @Test
    void test_simple() {
        // verify write
        DataBlockWriterTx writerTx = dataBlockFile.getDataBlockWriterTx();
        try (DataBlockWriter writer = writerTx.openWriter()) {
            writer.write(TestData.PAYLOAD_1008);
        }
        writerTx.commit();

        // Verify read
        try (DataBlockReader reader = dataBlockFile
                .openReader(DataBlockFile.FIRST_BLOCK)) {
            DataBlock dataBlock = reader.read();
            assertNotNull(dataBlock);
            DataBlockPayload payload = dataBlock.getPayload();
            assertNotNull(payload);
            assertEquals(TestData.PAYLOAD_1008, payload);
        }
    }
}
