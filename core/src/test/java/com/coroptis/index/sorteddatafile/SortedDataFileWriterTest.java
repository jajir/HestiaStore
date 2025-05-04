package com.coroptis.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.LoggingContext;
import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.datatype.TypeWriter;
import com.coroptis.index.directory.FileWriter;

@ExtendWith(MockitoExtension.class)
public class SortedDataFileWriterTest {

    private final static LoggingContext LOGGING_CONTEXT = new LoggingContext(
            "test_index");
    private static final Pair<String, Integer> PAIR_0 = Pair.of("key0", -100);
    private static final Pair<String, Integer> PAIR_1 = Pair.of("key1", 100);
    private static final Pair<String, Integer> PAIR_2 = Pair.of("key2", 200);
    private static final Pair<String, Integer> PAIR_3 = Pair.of("key3", 300);
    private static final Pair<String, Integer> PAIR_4 = Pair.of("key4", 400);

    private final static TypeDescriptor<String> stringTd = new TypeDescriptorString();

    @Mock
    private FileWriter fileWriter;

    @Mock
    private TypeWriter<Integer> valueWriter;

    @Test
    public void test_constructor_valueWriter_is_null() {
        final Exception e = assertThrows(NullPointerException.class,
                () -> new SortedDataFileWriter<>(LOGGING_CONTEXT, null,
                        fileWriter, stringTd));

        assertEquals("valueWriter is required", e.getMessage());
    }

    @Test
    void test_constructor_writer_is_null() {
        final Exception e = assertThrows(NullPointerException.class,
                () -> new SortedDataFileWriter<>(LOGGING_CONTEXT, valueWriter,
                        null, stringTd));

        assertEquals("fileWriter is required", e.getMessage());
    }

    @Test
    void test_constructor_keyTypeDescriptor_is_null() {
        final Exception e = assertThrows(NullPointerException.class,
                () -> new SortedDataFileWriter<>(LOGGING_CONTEXT, valueWriter,
                        fileWriter, null));

        assertEquals("keyTypeDescriptor is required", e.getMessage());
    }

    @Test
    void test_write_lower_key() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(
                LOGGING_CONTEXT, valueWriter, fileWriter, stringTd)) {
            writer.write(PAIR_1);
            final Exception e = assertThrows(IllegalArgumentException.class,
                    () -> writer.write(PAIR_0));
            assertTrue(e.getMessage()
                    .startsWith("Attempt to insers key in invalid order. "
                            + "Previous key is 'key1', inserted key is 'key0' and comparator "
                            + "is"));
        }
    }

    @Test
    void test_write_same_key() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(
                LOGGING_CONTEXT, valueWriter, fileWriter, stringTd)) {
            writer.write(PAIR_1);
            final Exception e = assertThrows(IllegalArgumentException.class,
                    () -> writer.write(PAIR_0));
            assertTrue(e.getMessage()
                    .startsWith("Attempt to insers key in invalid order. "
                            + "Previous key is 'key1', inserted key is 'key0' and comparator "
                            + "is"));
        }
    }

    @Test
    void test_writeFull_write_same_key() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(
                LOGGING_CONTEXT, valueWriter, fileWriter, stringTd)) {
            writer.writeFull(PAIR_1);
            final Exception e = assertThrows(IllegalArgumentException.class,
                    () -> writer.write(PAIR_0));
            assertTrue(e.getMessage()
                    .startsWith("Attempt to insers key in invalid order. "
                            + "Previous key is 'key1', inserted key is 'key0' and comparator "
                            + "is"));
        }
    }

    @Test
    public void test_write() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(
                LOGGING_CONTEXT, valueWriter, fileWriter, stringTd)) {
            writer.write(PAIR_1);
            verify(valueWriter).write(fileWriter, 100);
            writer.write(PAIR_2);
            verify(valueWriter).write(fileWriter, 200);
        }
    }

    @Test
    public void test_writeFull_all_writes_are_full() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(
                LOGGING_CONTEXT, valueWriter, fileWriter, stringTd)) {
            long ret = writer.writeFull(PAIR_1);
            assertEquals(0, ret);

            writer.write(PAIR_2);
            ret = writer.writeFull(PAIR_3);
            assertEquals(9, ret);

            ret = writer.writeFull(PAIR_4);
            assertEquals(15, ret);
        }
    }

    @Test
    public void test_writeFull_mixed() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(
                LOGGING_CONTEXT, valueWriter, fileWriter, stringTd)) {
            writer.write(PAIR_1);
            writer.write(PAIR_2);
            writer.write(PAIR_3);

            long ret = writer.writeFull(PAIR_4);
            assertEquals(12, ret);
        }
    }

}