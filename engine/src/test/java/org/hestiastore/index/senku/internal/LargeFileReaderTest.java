package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.hestiastore.index.senku.internal.LargeFileTestSupport.page;
import static org.hestiastore.index.senku.internal.LargeFileTestSupport.text;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.chunkstore.ChunkStoreWriter;
import org.hestiastore.index.chunkstore.ChunkStoreWriterTx;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LargeFileReaderTest {

    private MemDirectory directory;
    private LargeFilePosition secondPosition;
    private LargeFile file;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        final LargeFile writingFile = new LargeFile(directory, DATA_BLOCK_SIZE,
                2L, 0);
        final LargeFileWriterTx writer = writingFile.openWriterTx();
        writer.appendPage(page("first"), 1);
        secondPosition = writer.appendPage(page("second"), 1);
        writer.appendPage(page("third"), 1);
        final int partCount = writer.commit();
        file = new LargeFile(directory, DATA_BLOCK_SIZE, 2L, partCount);
    }

    @Test
    void readCrossesPartsAndKeepsStableEof() {
        try (LargeFileReader reader = file.openReader()) {
            assertEquals("first", text(reader.read()));
            assertEquals("second", text(reader.read()));
            assertEquals("third", text(reader.read()));
            assertNull(reader.read());
            assertNull(reader.read());
        }
    }

    @Test
    void positionedReadStartsAtExactPageAndCrossesParts() {
        try (LargeFileReader reader = file.openReader(secondPosition)) {
            assertEquals("second", text(reader.read()));
            assertEquals("third", text(reader.read()));
            assertNull(reader.read());
        }
    }

    @Test
    void emptyReaderReturnsStableEofWithoutOpeningPart() {
        final LargeFile empty = new LargeFile(new MemDirectory(),
                DATA_BLOCK_SIZE, 2L, 0);

        try (LargeFileReader reader = empty.openReader()) {
            assertNull(reader.read());
            assertNull(reader.read());
        }
    }

    @Test
    void closeIsIdempotentAndReadAfterCloseFails() {
        final LargeFileReader reader = file.openReader();

        reader.close();
        reader.close();

        assertThrows(IndexException.class, reader::read);
    }

    @Test
    void readRejectsWrongPageVersion() {
        final MemDirectory wrongVersionDirectory = new MemDirectory();
        final ChunkStoreWriterTx transaction = LargeFile
                .chunkStore(wrongVersionDirectory, DATA_BLOCK_SIZE, 0)
                .openWriteTx();
        final ChunkStoreWriter writer = transaction.open();
        writer.writeSequence(page("wrong"), 2);
        writer.close();
        transaction.commit();
        final LargeFile wrongVersionFile = new LargeFile(wrongVersionDirectory,
                DATA_BLOCK_SIZE, 2L, 1);

        try (LargeFileReader reader = wrongVersionFile.openReader()) {
            assertThrows(IndexException.class, reader::read);
        }
    }
}
