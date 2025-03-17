package com.coroptis.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Comparator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.directory.FileWriter;
import com.coroptis.index.directory.MemDirectory;

public class DiffKeyWriterTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    private MemDirectory directory;

    private FileWriter fileWriter;

    @BeforeEach
    public void setUp() {
        directory = new MemDirectory();
        fileWriter = directory.getFileWriter("duck");
    }

    @Test
    public void test_ordering_of_key() throws Exception {
        final DiffKeyWriter<Integer> diffWriter = new DiffKeyWriter<>(
                tdi.getConvertorToBytes(), Comparator.naturalOrder());
        diffWriter.write(1, fileWriter, true);
        diffWriter.write(2, fileWriter, false);
        diffWriter.write(3, fileWriter, false);
        diffWriter.write(4, fileWriter, false);
    }

    @Test
    public void test_ordering_same_keys_throw_exception() throws Exception {
        final DiffKeyWriter<Integer> diffWriter = new DiffKeyWriter<>(
                tdi.getConvertorToBytes(), Comparator.naturalOrder());
        diffWriter.write(1, fileWriter, true);
        diffWriter.write(2, fileWriter, true);
        diffWriter.write(3, fileWriter, true);

        assertThrows(IllegalArgumentException.class,
                () -> diffWriter.write(3, fileWriter, false));
    }

    @Test
    public void test_ordering_same_keys_throw_full_write_exception()
            throws Exception {
        final DiffKeyWriter<Integer> diffWriter = new DiffKeyWriter<>(
                tdi.getConvertorToBytes(), Comparator.naturalOrder());
        diffWriter.write(1, fileWriter, true);
        diffWriter.write(2, fileWriter, true);
        diffWriter.write(3, fileWriter, true);

        assertThrows(IllegalArgumentException.class,
                () -> diffWriter.write(3, fileWriter, true));
    }

    @Test
    public void test_ordering_smaller_key_than_previous_one_throw_exception()
            throws Exception {
        final DiffKeyWriter<Integer> diffWriter = new DiffKeyWriter<>(
                tdi.getConvertorToBytes(), Comparator.naturalOrder());
        diffWriter.write(1, fileWriter, true);

        assertThrows(IllegalArgumentException.class,
                () -> diffWriter.write(-1, fileWriter, false));
    }

}
