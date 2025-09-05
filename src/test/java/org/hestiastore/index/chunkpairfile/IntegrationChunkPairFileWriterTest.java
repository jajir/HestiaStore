package org.hestiastore.index.chunkpairfile;

import static org.junit.jupiter.api.Assertions.fail;

import org.hestiastore.index.TestData;
import org.hestiastore.index.chunkstore.ChunkStoreFile;
import org.hestiastore.index.chunkstore.ChunkStoreWriter;
import org.hestiastore.index.chunkstore.ChunkStoreWriterTx;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IntegrationChunkPairFileWriterTest {

    private final static int BLOCK_SIZE = 1024;

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
        final ChunkStoreWriterTx chunkStoreWriterTx = chunkStoreFile
                .openWriteTx();
        ChunkStoreWriter chunkStoreWriter = chunkStoreWriterTx.openWriter();

        final ChunkPairFileWriter<Integer, String> chunkPairFileWriter = new ChunkPairFileWriter<>(
                chunkStoreWriter, TestData.TYPE_DESCRIPTOR_INTEGER,
                TestData.TYPE_DESCRIPTOR_STRING);
        chunkPairFileWriter.write(TestData.PAIR1);
        chunkPairFileWriter.write(TestData.PAIR2);
        chunkPairFileWriter.write(TestData.PAIR3);

        chunkStoreWriter.close();
        chunkStoreWriterTx.commit();
    }

}
