package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.TestData;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.datablockfile.DataBlockFile;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class ChunkStoreWriterTxTest {

    private static final DataBlockSize DATA_BLOCK_SIZE = DataBlockSize
            .ofDataBlockSize(1024);

    @Test
    void openWriteCloseAndCommitWritesUsingProvidedFilters() {
        final MemDirectory directory = new MemDirectory();
        final CountingChunkFilter filter = new CountingChunkFilter();
        final ChunkStoreWriterTx tx = new ChunkStoreWriterTx(
                new DataBlockFile(directory, "chunk-writer-tx-test",
                        DATA_BLOCK_SIZE),
                DATA_BLOCK_SIZE, List.of(filter));
        final CellPosition position;

        try (ChunkStoreWriter writer = tx.open()) {
            position = writer.writeSequence(
                    TestData.CHUNK_PAYLOAD_9.getBytesSequence(), 7);
        }
        tx.commit();

        assertEquals(1, filter.getApplyCount());
        try (ChunkStoreReader reader = new ChunkStoreFile(directory,
                "chunk-writer-tx-test", DATA_BLOCK_SIZE,
                List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing())).openReader(position)) {
            final Chunk chunk = reader.read();
            assertNotNull(chunk);
            assertTrue(ByteSequences.contentEquals(TestData.BYTES_9,
                    chunk.getPayloadSequence()));
        }
    }

    @Test
    void openRejectsSecondInvocation() {
        final ChunkStoreWriterTx tx = new ChunkStoreWriterTx(
                new DataBlockFile(new MemDirectory(), "chunk-writer-open-test",
                        DATA_BLOCK_SIZE),
                DATA_BLOCK_SIZE, List.of(new ChunkFilterDoNothing()));
        final ChunkStoreWriter writer = tx.open();
        try {
            final IllegalStateException exception = assertThrows(
                    IllegalStateException.class, tx::open);
            assertEquals("Resource already opened", exception.getMessage());
        } finally {
            writer.close();
        }
    }

    @Test
    void commitRejectsWhenWriterWasNotOpened() {
        final ChunkStoreWriterTx tx = new ChunkStoreWriterTx(
                new DataBlockFile(new MemDirectory(), "chunk-writer-no-open",
                        DATA_BLOCK_SIZE),
                DATA_BLOCK_SIZE, List.of(new ChunkFilterDoNothing()));

        final IllegalStateException exception = assertThrows(
                IllegalStateException.class, tx::commit);

        assertEquals("Resource has not been opened", exception.getMessage());
    }

    @Test
    void commitRejectsWhenWriterIsStillOpen() {
        final ChunkStoreWriterTx tx = new ChunkStoreWriterTx(
                new DataBlockFile(new MemDirectory(), "chunk-writer-open",
                        DATA_BLOCK_SIZE),
                DATA_BLOCK_SIZE, List.of(new ChunkFilterDoNothing()));
        final ChunkStoreWriter writer = tx.open();
        try {
            final IllegalStateException exception = assertThrows(
                    IllegalStateException.class, tx::commit);
            assertEquals("Resource must be closed before commit",
                    exception.getMessage());
        } finally {
            writer.close();
        }
    }

    @Test
    void commitRejectsSecondInvocation() {
        final ChunkStoreWriterTx tx = new ChunkStoreWriterTx(
                new DataBlockFile(new MemDirectory(), "chunk-writer-commit",
                        DATA_BLOCK_SIZE),
                DATA_BLOCK_SIZE, List.of(new ChunkFilterDoNothing()));
        try (ChunkStoreWriter writer = tx.open()) {
            writer.writeSequence(TestData.CHUNK_PAYLOAD_9.getBytesSequence(), 3);
        }
        tx.commit();

        final IllegalStateException exception = assertThrows(
                IllegalStateException.class, tx::commit);

        assertEquals("Transaction already committed", exception.getMessage());
    }

    private static final class CountingChunkFilter implements ChunkFilter {

        private int applyCount;

        @Override
        public ChunkData apply(final ChunkData input) {
            applyCount++;
            return input;
        }

        private int getApplyCount() {
            return applyCount;
        }
    }
}
