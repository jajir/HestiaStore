package org.hestiastore.index.chunkentryfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.hestiastore.index.AbstractDataTest;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.Entry;
import org.hestiastore.index.TestData;
import org.hestiastore.index.chunkstore.Chunk;
import org.hestiastore.index.chunkstore.ChunkHeader;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.sorteddatafile.SortedDataFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SingleChunkEntryIteratorTest {

    private static final String FILE_NAME = "chunkentryfilewriter-test";

    private MemDirectory directory;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
    }

    @AfterEach
    void tearDown() {
        directory = null;
    }

    Chunk makeChunkFromEntryList(final List<Entry<Integer, String>> entryList) {
        final SortedDataFile<Integer, String> sortedDataFile = SortedDataFile
                .<Integer, String>builder() //
                .withAsyncDirectory(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(directory)) //
                .withFileName(FILE_NAME)//
                .withKeyTypeDescriptor(TestData.TYPE_DESCRIPTOR_INTEGER) //
                .withValueTypeDescriptor(TestData.TYPE_DESCRIPTOR_STRING) //
                .withDiskIoBufferSize(1024)//
                .build();
        sortedDataFile.openWriterTx().execute(writer -> {
            entryList.forEach(writer::write);
        });

        final Bytes fileBytes = directory.getFileBytes(FILE_NAME);
        return Chunk.of(ChunkHeader.of(ChunkHeader.MAGIC_NUMBER, 1,
                fileBytes.length(), 321L), fileBytes);
    }

    @Test
    void test_simple() {
        final Chunk chunk = makeChunkFromEntryList(TestData.ENTRY_LIST_3);
        SingleChunkEntryIterator<Integer, String> iterator = new SingleChunkEntryIterator<>(
                chunk, TestData.TYPE_DESCRIPTOR_INTEGER,
                TestData.TYPE_DESCRIPTOR_STRING);

        AbstractDataTest.verifyIteratorData(TestData.ENTRY_LIST_3, iterator);
    }

    @Test
    void test_empty() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> makeChunkFromEntryList(TestData.ENTRY_LIST_EMPTY));
        assertEquals("Property 'payloadLength' must be greater than 0",
                exception.getMessage());
    }

}
