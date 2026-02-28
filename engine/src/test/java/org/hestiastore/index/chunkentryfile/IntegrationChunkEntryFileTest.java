package org.hestiastore.index.chunkentryfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.TestData;
import org.hestiastore.index.chunkstore.CellPosition;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.chunkstore.ChunkStoreFile;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileReaderSeekable;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IntegrationChunkEntryFileTest {

    private static final DataBlockSize BLOCK_SIZE = DataBlockSize
            .ofDataBlockSize(1024);

    private static final String FILE_NAME = "chunkentryfilewriter-test";

    private Directory directory;

    private ChunkEntryFile<Integer, String> chunkPairFile;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        ChunkStoreFile chunkStoreFile = new ChunkStoreFile(
                directory,
                FILE_NAME, BLOCK_SIZE,
                List.of(new ChunkFilterMagicNumberWriting(),
                        new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        chunkPairFile = new ChunkEntryFile<>(chunkStoreFile,
                TestData.TYPE_DESCRIPTOR_INTEGER,
                TestData.TYPE_DESCRIPTOR_STRING, BLOCK_SIZE);
    }

    @AfterEach
    void tearDown() {
        directory = null;
        chunkPairFile = null;
    }

    @Test
    void test_simple() {
        // Write data
        ChunkEntryFileWriterTx<Integer, String> writerTx = chunkPairFile
                .openWriterTx();
        CellPosition position = null;
        try (final ChunkEntryFileWriter<Integer, String> chunkPairFileWriter = writerTx
                .openWriter()) {
            chunkPairFileWriter.write(TestData.ENTRY1);
            chunkPairFileWriter.write(TestData.ENTRY2);
            chunkPairFileWriter.write(TestData.ENTRY3);
            position = chunkPairFileWriter.flush();
        }
        writerTx.commit();
        assertEquals(0, position.getValue());

        // Read data
        Iterator<Entry<Integer, String>> iterator = chunkPairFile
                .openIterator();
        assertTrue(iterator.hasNext());
        Entry<Integer, String> entry = iterator.next();
        assertEquals(TestData.ENTRY1, entry);
        assertTrue(iterator.hasNext());
        entry = iterator.next();
        assertEquals(TestData.ENTRY2, entry);
        assertTrue(iterator.hasNext());
        entry = iterator.next();
        assertEquals(TestData.ENTRY3, entry);
        assertFalse(iterator.hasNext());
    }

    @Test
    void search_at_position_uses_lookup_lane() {
        final ChunkEntryFileWriterTx<Integer, String> writerTx = chunkPairFile
                .openWriterTx();
        CellPosition position = null;
        try (ChunkEntryFileWriter<Integer, String> writer = writerTx
                .openWriter()) {
            writer.write(TestData.ENTRY1);
            writer.write(TestData.ENTRY2);
            writer.write(TestData.ENTRY3);
            position = writer.flush();
        }
        writerTx.commit();

        try (FileReaderSeekable seekableReader = directory
                .getFileReaderSeekable(FILE_NAME)) {
            assertEquals(TestData.ENTRY2.getValue(),
                    chunkPairFile.searchAtPosition(TestData.ENTRY2.getKey(),
                            position.getValue(), 3, Integer::compareTo,
                            seekableReader));
        }

        try (FileReaderSeekable seekableReader = directory
                .getFileReaderSeekable(FILE_NAME)) {
            assertNull(chunkPairFile.searchAtPosition(999, position.getValue(),
                    3, Integer::compareTo, seekableReader));
        }
    }

}
