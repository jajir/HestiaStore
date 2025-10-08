package org.hestiastore.index.chunkpairfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.List;

import org.hestiastore.index.Pair;
import org.hestiastore.index.TestData;
import org.hestiastore.index.chunkstore.CellPosition;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.chunkstore.ChunkStoreFile;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IntegrationChunkPairFileTest {

    private static final DataBlockSize BLOCK_SIZE = DataBlockSize
            .ofDataBlockSize(1024);

    private static final String FILE_NAME = "chunkpairfilewriter-test";

    private Directory directory;

    private ChunkPairFile<Integer, String> chunkPairFile;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        ChunkStoreFile chunkStoreFile = new ChunkStoreFile(directory, FILE_NAME,
                BLOCK_SIZE,
                List.of(new ChunkFilterMagicNumberWriting(),
                        new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        chunkPairFile = new ChunkPairFile<>(chunkStoreFile,
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
        ChunkPairFileWriterTx<Integer, String> writerTx = chunkPairFile
                .openWriterTx();
        CellPosition position = null;
        try (final ChunkPairFileWriter<Integer, String> chunkPairFileWriter = writerTx
                .openWriter()) {
            chunkPairFileWriter.write(TestData.PAIR1);
            chunkPairFileWriter.write(TestData.PAIR2);
            chunkPairFileWriter.write(TestData.PAIR3);
            position = chunkPairFileWriter.flush();
        }
        writerTx.commit();
        assertEquals(0, position.getValue());

        // Read data
        Iterator<Pair<Integer, String>> iterator = chunkPairFile.openIterator();
        assertTrue(iterator.hasNext());
        Pair<Integer, String> pair = iterator.next();
        assertEquals(TestData.PAIR1, pair);
        assertTrue(iterator.hasNext());
        pair = iterator.next();
        assertEquals(TestData.PAIR2, pair);
        assertTrue(iterator.hasNext());
        pair = iterator.next();
        assertEquals(TestData.PAIR3, pair);
        assertFalse(iterator.hasNext());
    }

}
