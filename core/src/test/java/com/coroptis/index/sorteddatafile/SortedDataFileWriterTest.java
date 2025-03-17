package com.coroptis.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeWriter;
import com.coroptis.index.directory.FileWriter;

@ExtendWith(MockitoExtension.class)
public class SortedDataFileWriterTest {

    private static final int BUFFER_SIZE = 1024;

    private static final Pair<String, Integer> PAIR_1 = Pair.of("key1", 100);;
    private static final Pair<String, Integer> PAIR_2 = Pair.of("key2", 200);;

    @Mock
    private FileWriter fileWriter;

    @Mock
    private DiffKeyWriter<String> diffKeyWriter;

    @Mock
    private TypeWriter<Integer> valueWriter;

    @Test
    public void test_constructor_valueWriter_is_null() {
        final Exception e = assertThrows(NullPointerException.class,
                () -> new SortedDataFileWriter<>(null, BUFFER_SIZE, fileWriter,
                        diffKeyWriter));

        assertEquals("valueWriter is required", e.getMessage());
    }

    @Test
    void test_constructor_writer_is_null() {
        final Exception e = assertThrows(NullPointerException.class,
                () -> new SortedDataFileWriter<>(valueWriter, BUFFER_SIZE, null, diffKeyWriter));

        assertEquals("writer is required", e.getMessage());
    }

    @Test
    void test_constructor_diffKeyWriter_is_null() {
        final Exception e = assertThrows(NullPointerException.class,
                () -> new SortedDataFileWriter<>(valueWriter, BUFFER_SIZE, fileWriter, null));

        assertEquals("diffKeyWriter is required", e.getMessage());
    }

    @Test
    public void test_write() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(valueWriter, BUFFER_SIZE,
                fileWriter,
                diffKeyWriter)) {
            writer.write(PAIR_1);
            verify(diffKeyWriter).write("key1", false);
            verify(valueWriter).write(fileWriter, 100);
        }
    }

    @Test
    public void test_writeFull() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(valueWriter, BUFFER_SIZE,
                fileWriter,
                diffKeyWriter)) {
            when(diffKeyWriter.write("key1", true)).thenReturn(37);
            when(valueWriter.write(fileWriter, 100)).thenReturn(23);
            long ret = writer.writeFull(PAIR_1);

            assertEquals(0, ret);

            when(diffKeyWriter.write("key2", true)).thenReturn(13);
            when(valueWriter.write(fileWriter, 200)).thenReturn(77);
            ret = writer.writeFull(PAIR_2);

            assertEquals(60, ret);
        }
    }
}