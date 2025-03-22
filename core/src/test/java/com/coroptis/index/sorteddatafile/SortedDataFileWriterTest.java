package com.coroptis.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.datatype.TypeWriter;
import com.coroptis.index.directory.FileWriter;

@ExtendWith(MockitoExtension.class)
public class SortedDataFileWriterTest {

    private static final Pair<String, Integer> PAIR_1 = Pair.of("key1", 100);
    private static final Pair<String, Integer> PAIR_2 = Pair.of("key2", 200);
    private static final Pair<String, Integer> PAIR_3 = Pair.of("key0", 300);

    private final static TypeDescriptor<String> stringTd = new TypeDescriptorString();

    @Mock
    private FileWriter fileWriter;

    @Mock
    private TypeWriter<Integer> valueWriter;

    @Test
    public void test_constructor_valueWriter_is_null() {
        final Exception e = assertThrows(NullPointerException.class,
                () -> new SortedDataFileWriter<>(null, fileWriter,
                        stringTd));

        assertEquals("valueWriter is required", e.getMessage());
    }

    @Test
    void test_constructor_writer_is_null() {
        final Exception e = assertThrows(NullPointerException.class,
                () -> new SortedDataFileWriter<>(valueWriter, null, stringTd));

        assertEquals("fileWriter is required", e.getMessage());
    }

    @Test
    void test_constructor_keyTypeDescriptor_is_null() {
        final Exception e = assertThrows(NullPointerException.class,
                () -> new SortedDataFileWriter<>(valueWriter, fileWriter, null));

        assertEquals("keyTypeDescriptor is required", e.getMessage());
    }

    @Test
    void test_write_lower_key() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(valueWriter, fileWriter,
                stringTd)) {
            writer.write(PAIR_1);
            final Exception e = assertThrows(IllegalArgumentException.class,
                    () -> writer.write(PAIR_3));
            assertTrue(e.getMessage().startsWith("Attempt to insers key in invalid order. "
                    + "Previous key is 'key1', inserted key is 'key0' and comparator "
                    + "is"));
        }
    }

    @Test
    void test_write_same_key() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(valueWriter, fileWriter,
                stringTd)) {
            writer.write(PAIR_1);
            final Exception e = assertThrows(IllegalArgumentException.class,
                    () -> writer.write(PAIR_3));
            assertTrue(e.getMessage().startsWith("Attempt to insers key in invalid order. "
                    + "Previous key is 'key1', inserted key is 'key0' and comparator "
                    + "is"));
        }
    }

    @Test
    void test_writeFull_write_same_key() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(valueWriter, fileWriter,
                stringTd)) {
            writer.writeFull(PAIR_1);
            final Exception e = assertThrows(IllegalArgumentException.class,
                    () -> writer.write(PAIR_3));
            assertTrue(e.getMessage().startsWith("Attempt to insers key in invalid order. "
                    + "Previous key is 'key1', inserted key is 'key0' and comparator "
                    + "is"));
        }
    }

    @Test
    public void test_write() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(valueWriter, fileWriter,
                stringTd)) {
            writer.write(PAIR_1);
            verify(valueWriter).write(fileWriter, 100);
        }
    }

    //TODO add tests write & writeFull

    @Test
    @Disabled
    public void test_writeFull() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(valueWriter, fileWriter,
                stringTd)) {
            when(valueWriter.write(fileWriter, 100)).thenReturn(23);
            long ret = writer.writeFull(PAIR_1);

            assertEquals(0, ret);

            when(valueWriter.write(fileWriter, 200)).thenReturn(77);
            ret = writer.writeFull(PAIR_2);

            assertEquals(60, ret);
        }
    }
    
}