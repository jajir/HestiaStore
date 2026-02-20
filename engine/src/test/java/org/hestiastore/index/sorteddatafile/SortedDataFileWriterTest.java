package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.hestiastore.index.Entry;
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

    private static final Entry<String, Integer> ENTRY_0 = Entry.of("key0", -100);
    private static final Entry<String, Integer> ENTRY_1 = Entry.of("key1", 100);
    private static final Entry<String, Integer> ENTRY_2 = Entry.of("key2", 200);
    private static final Entry<String, Integer> ENTRY_3 = Entry.of("key3", 300);
    private static final Entry<String, Integer> ENTRY_4 = Entry.of("key4", 400);

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
            writer.write(ENTRY_1);
            final Exception e = assertThrows(IllegalArgumentException.class,
                    () -> writer.write(ENTRY_0));
            final String prevB64 = Base64.getEncoder().encodeToString(
                    "key1".getBytes(StandardCharsets.UTF_8));
            final String insB64 = Base64.getEncoder().encodeToString(
                    "key0".getBytes(StandardCharsets.UTF_8));
            assertTrue(e.getMessage().startsWith(
                    "Attempt to insert key in invalid order. previous(Base64)='"
                            + prevB64 + "', inserted(Base64)='" + insB64
                            + "', comparator='"));
        }
    }

    @Test
    void test_write_same_key() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(
                valueWriter, fileWriter, stringTd)) {
            writer.write(ENTRY_1);
            final Exception e = assertThrows(IllegalArgumentException.class,
                    () -> writer.write(ENTRY_0));
            final String prevB64 = Base64.getEncoder().encodeToString(
                    "key1".getBytes(StandardCharsets.UTF_8));
            final String insB64 = Base64.getEncoder().encodeToString(
                    "key0".getBytes(StandardCharsets.UTF_8));
            assertTrue(e.getMessage().startsWith(
                    "Attempt to insert key in invalid order. previous(Base64)='"
                            + prevB64 + "', inserted(Base64)='" + insB64
                            + "', comparator='"));
        }
    }

    @Test
    void test_write_write_same_key() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(
                valueWriter, fileWriter, stringTd)) {
            writer.write(ENTRY_1);
            final Exception e = assertThrows(IllegalArgumentException.class,
                    () -> writer.write(ENTRY_0));
            final String prevB64 = Base64.getEncoder().encodeToString(
                    "key1".getBytes(StandardCharsets.UTF_8));
            final String insB64 = Base64.getEncoder().encodeToString(
                    "key0".getBytes(StandardCharsets.UTF_8));
            assertTrue(e.getMessage().startsWith(
                    "Attempt to insert key in invalid order. previous(Base64)='"
                            + prevB64 + "', inserted(Base64)='" + insB64
                            + "', comparator='"));
        }
    }

    @Test
    void test_write() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(
                valueWriter, fileWriter, stringTd)) {
            writer.write(ENTRY_1);
            verify(valueWriter).write(fileWriter, 100);
            writer.write(ENTRY_2);
            verify(valueWriter).write(fileWriter, 200);
        }
    }

    @Test
    void test_write_all_writes_are_full() {
        try (SortedDataFileWriter<String, Integer> writer = new SortedDataFileWriter<>(
                valueWriter, fileWriter, stringTd)) {
            writer.write(ENTRY_1);
            verify(valueWriter, times(1)).write(fileWriter, 100);

            writer.write(ENTRY_2);
            verify(valueWriter, times(1)).write(fileWriter, 200);

            writer.write(ENTRY_3);
            verify(valueWriter, times(1)).write(fileWriter, 300);

            writer.write(ENTRY_4);
            verify(valueWriter, times(1)).write(fileWriter, 400);
        }
    }

}
