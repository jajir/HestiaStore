package org.hestiastore.index.datablockfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.hestiastore.index.TestData;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IntegrationDataBlockFileTest {

    private static final DataBlockSize BLOCK_SIZE = DataBlockSize
            .ofDataBlockSize(1024);

    private static final String FILE_NAME = "chunkentryfilewriter-test";

    private Directory directory;

    private DataBlockFile dataBlockFile;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        dataBlockFile = new DataBlockFile(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(directory),
                FILE_NAME, BLOCK_SIZE);
    }

    @AfterEach
    void tearDown() {
        directory = null;
        dataBlockFile = null;
    }

    @Test
    void test_write_and_read_one_data_block() {
        // verify write
        DataBlockWriterTx writerTx = dataBlockFile.openWriterTx();
        try (DataBlockWriter writer = writerTx.open()) {
            writer.write(TestData.PAYLOAD_1008);
        }
        writerTx.commit();

        // Verify read
        try (DataBlockReader reader = dataBlockFile
                .openReader(DataBlockFile.FIRST_BLOCK)) {
            verifyBlock(reader.read(), TestData.PAYLOAD_1008);
            assertNull(reader.read());
        }
    }

    @Test
    void test_write_and_read_two_data_block() {
        // verify write
        DataBlockWriterTx writerTx = dataBlockFile.openWriterTx();
        try (DataBlockWriter writer = writerTx.open()) {
            writer.write(TestData.PAYLOAD_1008);
            writer.write(TestData.PAYLOAD_1008_2);
        }
        writerTx.commit();

        // Verify read
        try (DataBlockReader reader = dataBlockFile
                .openReader(DataBlockFile.FIRST_BLOCK)) {
            verifyBlock(reader.read(), TestData.PAYLOAD_1008);
            verifyBlock(reader.read(), TestData.PAYLOAD_1008_2);
            assertNull(reader.read());
        }
    }

    @Test
    void test_write_and_read_second_data_block() {
        // verify write
        DataBlockWriterTx writerTx = dataBlockFile.openWriterTx();
        try (DataBlockWriter writer = writerTx.open()) {
            writer.write(TestData.PAYLOAD_1008);
            writer.write(TestData.PAYLOAD_1008_2);
        }
        writerTx.commit();

        // Verify read
        try (DataBlockReader reader = dataBlockFile
                .openReader(DataBlockPosition.of(1008 + 16))) {
            verifyBlock(reader.read(), TestData.PAYLOAD_1008_2);
            assertNull(reader.read());
        }
    }

    private void verifyBlock(final DataBlock dataBlock,
            DataBlockPayload expectedPayload) {
        assertNotNull(dataBlock);
        final DataBlockPayload payload = dataBlock.getPayload();
        assertNotNull(payload);
        assertEquals(expectedPayload, payload);
    }
}
