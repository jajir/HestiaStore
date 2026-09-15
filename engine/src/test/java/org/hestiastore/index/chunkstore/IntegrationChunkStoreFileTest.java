package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Random;
import java.util.zip.CRC32;

import org.hestiastore.index.TestData;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.bytes.ConcatenatedByteSequence;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.luben.zstd.Zstd;

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

    @Test
    void test_zstd_round_trip_across_multiple_blocks_preserves_encoded_bytes() {
        final byte[] payload = createMultiBlockCompressiblePayload();
        final byte[] expectedCompressed = Zstd.compress(payload, 3);
        assertTrue(expectedCompressed.length > 3
                * DATABLOCK_SIZE.getPayloadSize());
        assertTrue(expectedCompressed.length < payload.length);
        final ByteSequence nestedPayload = ByteSequences.concatNonEmpty(List.of(
                ByteSequences.viewOf(payload, 0, 31),
                ByteSequences.viewOf(payload, 31, 2048),
                ByteSequences.viewOf(payload, 2048, 12000),
                ByteSequences.viewOf(payload, 12000, payload.length)));
        chunkStoreFile = new ChunkStoreFile(directory, FILE_NAME, DATABLOCK_SIZE,
                List.of(new ChunkFilterMagicNumberWriting(),
                        new ChunkFilterZstdCompress(3),
                        new ChunkFilterCrc32Writing()),
                List.of(new ChunkFilterMagicNumberValidation(),
                        new ChunkFilterCrc32Validation(),
                        new ChunkFilterZstdDecompress()));
        final ChunkStoreWriterTx writerTx = chunkStoreFile.openWriteTx();
        final CellPosition position;
        try (ChunkStoreWriter writer = writerTx.open()) {
            position = writer.writeSequence(nestedPayload, VERSION);
        }
        writerTx.commit();

        final ChunkStoreFile encodedFile = new ChunkStoreFile(directory,
                FILE_NAME, DATABLOCK_SIZE, List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterMagicNumberValidation(),
                        new ChunkFilterCrc32Validation()));
        try (ChunkStoreReader reader = encodedFile.openReader(position)) {
            final Chunk stored = reader.read();
            assertNotNull(stored);
            assertInstanceOf(ConcatenatedByteSequence.class,
                    stored.getPayloadSequence());
            assertArrayEquals(expectedCompressed,
                    stored.getPayloadSequence().toByteArray());
            final CRC32 crc = new CRC32();
            crc.update(expectedCompressed);
            assertEquals(ChunkHeader.of(ChunkHeader.MAGIC_NUMBER, VERSION,
                    expectedCompressed.length, crc.getValue(),
                    ChunkFilterMagicNumberWriting.FLAG_MASK
                            | ChunkFilterZstdCompress.FLAG_COMPRESSED),
                    stored.getHeader());
        }
        try (ChunkStoreReader reader = chunkStoreFile.openReader(position)) {
            final Chunk decoded = reader.read();
            assertNotNull(decoded);
            assertEquals(VERSION, decoded.getHeader().getVersion());
            assertArrayEquals(payload,
                    decoded.getPayloadSequence().toByteArray());
            assertNull(reader.read());
        }
    }

    private static byte[] createMultiBlockCompressiblePayload() {
        final byte[] pattern = new byte[4096];
        new Random(42).nextBytes(pattern);
        final byte[] payload = new byte[4 * pattern.length];
        for (int offset = 0; offset < payload.length; offset += pattern.length) {
            System.arraycopy(pattern, 0, payload, offset, pattern.length);
        }
        return payload;
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
