package com.coroptis.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Comparator;

import org.junit.jupiter.api.Test;

import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.directory.MemDirectory;

public class DiffKeyWriterTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    @Test
    public void test_ordering_of_key() throws Exception {
        final MemDirectory directory = new MemDirectory();
        try (final DiffKeyWriter<Integer> diffWriter = new DiffKeyWriter<>(
                tdi.getConvertorToBytes(), Comparator.naturalOrder(), directory.getFileWriter("duck"))) {
            diffWriter.write( 1, true);
            diffWriter.write( 2, false);
            diffWriter.write( 3, false);
            diffWriter.write( 4, false);
        }
    }

    @Test
    public void test_ordering_same_keys_throw_exception() throws Exception {
        final MemDirectory directory = new MemDirectory();
        try (final DiffKeyWriter<Integer> diffWriter = new DiffKeyWriter<>(
                tdi.getConvertorToBytes(), Comparator.naturalOrder(), directory.getFileWriter("duck"))) {
            diffWriter.write( 1, true);
            diffWriter.write( 2, true);
            diffWriter.write( 3, true);

            assertThrows(IllegalArgumentException.class,
                    () -> diffWriter.write(3, false));
        }
    }

    @Test
    public void test_ordering_same_keys_throw_full_write_exception()
            throws Exception {
        final MemDirectory directory = new MemDirectory();
        try (final DiffKeyWriter<Integer> diffWriter = new DiffKeyWriter<>(
                tdi.getConvertorToBytes(), Comparator.naturalOrder(), directory.getFileWriter("duck"))) {
            diffWriter.write( 1, true);
            diffWriter.write( 2, true);
            diffWriter.write(3, true);

            assertThrows(IllegalArgumentException.class,
                    () -> diffWriter.write( 3, true));
        }
    }

    @Test
    public void test_ordering_smaller_key_than_previous_one_throw_exception()
            throws Exception {
        final MemDirectory directory = new MemDirectory();
        try (final DiffKeyWriter<Integer> diffWriter = new DiffKeyWriter<>(
                tdi.getConvertorToBytes(), Comparator.naturalOrder(), directory.getFileWriter("duck"))) {
            diffWriter.write( 1, true);

            assertThrows(IllegalArgumentException.class,
                    () -> diffWriter.write( -1, false));
        }
    }

    @SuppressWarnings("resource")
    @Test
    public void test_constructor_null_writer_throws_exception() {
            final Exception e = assertThrows(NullPointerException.class, () -> {
                    new DiffKeyWriter<>(tdi.getConvertorToBytes(), Comparator.naturalOrder(), null);
            });

            assertEquals("FileWriter can't be null", e.getMessage());
    }

}
