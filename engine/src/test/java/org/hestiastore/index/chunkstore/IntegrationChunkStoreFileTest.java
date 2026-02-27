package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.TestData;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IntegrationChunkStoreFileTest {

    private static final DataBlockSize DATABLOCK_SIZE = DataBlockSize
            .ofDataBlockSize(1024);

    private static final int VERSION = 3;

    private static final String FILE_NAME = "chunkentryfilewriter-test";

    private Directory directory;

    private ChunkStoreFile chunkStoreFile;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        chunkStoreFile = new ChunkStoreFile(
                directory,
                FILE_NAME, DATABLOCK_SIZE,
                List.of(new ChunkFilterMagicNumberWriting(),
                        new ChunkFilterCrc32Writing(),
                        new ChunkFilterDoNothing()),
                List.of(new ChunkFilterCrc32Validation(),
                        new ChunkFilterDoNothing()));
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
        try (ChunkStoreWriter writer = writerTx.open()) {
            position = writer.writeSequence(
                    TestData.CHUNK_PAYLOAD_154.getBytesSequence(), VERSION);
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
        try (ChunkStoreWriter writer = writerTx.open()) {
            positions[0] = writer.writeSequence(
                    TestData.CHUNK_PAYLOAD_1008.getBytesSequence(), VERSION);
            positions[1] = writer.writeSequence(
                    TestData.CHUNK_PAYLOAD_1008.getBytesSequence(), VERSION);
            positions[2] = writer.writeSequence(
                    TestData.CHUNK_PAYLOAD_1008.getBytesSequence(), VERSION);
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
            assertNotNull(chunk.getPayloadSequence());
            assertTrue(ByteSequences.contentEquals(
                    expectedPayload.getBytesSequence(),
                    chunk.getPayloadSequence()));
        }
    }

}
