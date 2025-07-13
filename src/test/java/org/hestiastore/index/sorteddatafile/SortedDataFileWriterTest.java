package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;

import org.hestiastore.index.Pair;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.directory.FileWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SortedDataFileWriterTest {

    private static final Pair<String, Integer> PAIR_0 = Pair.of("key0", -100);
    private static final Pair<String, Integer> PAIR_1 = Pair.of("key1", 100);
    private static final Pair<String, Integer> PAIR_2 = Pair.of("key2", 200);
    private static final Pair<String, Integer> PAIR_3 = Pair.of("key3", 300);
    private static final Pair<String, Integer> PAIR_4 = Pair.of("key4", 400);

    private static final TypeDescriptor<String> stringTd = new TypeDescriptorShortString();

    @Mock
    private FileWriter fileWriter;

    @Mock
    private TypeWriter<Integer> valueWriter;

    @Test
    void test_constructor_valueWriter_is_null() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new SortedDataFileWriter<>(null, fileWriter, stringTd));

        assertEquals("Property 'valueWriter' must not be null.",
                e.getMessage());
    }

    @Test
    void test_constructor_writer_is_null() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new SortedDataFileWriter<>(valueWriter, null, stringTd));

        assertEquals("Property 'fileWriter' must not be null.", e.getMessage());
    }

    @Test
    void test_constructor_keyTypeDescriptor_is_null() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new SortedDataFileWriter<>(valueWriter, fileWriter,
                        null));

        assertEquals("Property 'keyTypeDescriptor' must not be null.",
                e.getMessage());
    }

    @Test
    void test_write_lower_key() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(
                valueWriter, fileWriter, stringTd)) {
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
                valueWriter, fileWriter, stringTd)) {
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
                valueWriter, fileWriter, stringTd)) {
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
    void test_write() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(
                valueWriter, fileWriter, stringTd)) {
            writer.write(PAIR_1);
            verify(valueWriter).write(fileWriter, 100);
            writer.write(PAIR_2);
            verify(valueWriter).write(fileWriter, 200);
        }
    }

    @Test
    void test_writeFull_all_writes_are_full() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(
                valueWriter, fileWriter, stringTd)) {
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
    void test_writeFull_mixed() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(
                valueWriter, fileWriter, stringTd)) {
            writer.write(PAIR_1);
            writer.write(PAIR_2);
            writer.write(PAIR_3);

            long ret = writer.writeFull(PAIR_4);
            assertEquals(12, ret);
        }
    }

}