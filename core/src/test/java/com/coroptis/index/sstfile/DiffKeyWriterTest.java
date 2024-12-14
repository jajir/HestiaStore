package com.coroptis.index.sstfile;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Comparator;

import org.junit.jupiter.api.Test;

import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.directory.FileWriter;
import com.coroptis.index.directory.MemDirectory;

public class DiffKeyWriterTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    @Test
    public void test_ordering_of_key() throws Exception {
        final DiffKeyWriter<Integer> diffWriter = new DiffKeyWriter<>(
                tdi.getConvertorToBytes(), Comparator.naturalOrder());
        final MemDirectory directory = new MemDirectory();
        FileWriter fileWriter = directory.getFileWriter("duck");
        diffWriter.write(fileWriter, 1, true);
        diffWriter.write(fileWriter, 2);
        diffWriter.write(fileWriter, 3);
        diffWriter.write(fileWriter, 4);
    }

    @Test
    public void test_ordering_same_keys_throw_exception() throws Exception {
        final DiffKeyWriter<Integer> diffWriter = new DiffKeyWriter<>(
                tdi.getConvertorToBytes(), Comparator.naturalOrder());
        final MemDirectory directory = new MemDirectory();
        FileWriter fileWriter = directory.getFileWriter("duck");
        diffWriter.write(fileWriter, 1, true);
        diffWriter.write(fileWriter, 2, true);
        diffWriter.write(fileWriter, 3, true);

        assertThrows(IllegalArgumentException.class,
                () -> diffWriter.write(fileWriter, 3));
    }

    @Test
    public void test_ordering_same_keys_throw_full_write_exception()
            throws Exception {
        final DiffKeyWriter<Integer> diffWriter = new DiffKeyWriter<>(
                tdi.getConvertorToBytes(), Comparator.naturalOrder());
        final MemDirectory directory = new MemDirectory();
        FileWriter fileWriter = directory.getFileWriter("duck");
        diffWriter.write(fileWriter, 1, true);
        diffWriter.write(fileWriter, 2, true);
        diffWriter.write(fileWriter, 3, true);

        assertThrows(IllegalArgumentException.class,
                () -> diffWriter.write(fileWriter, 3, true));
    }

    @Test
    public void test_ordering_smaller_key_than_previous_one_throw_exception()
            throws Exception {
        final DiffKeyWriter<Integer> diffWriter = new DiffKeyWriter<>(
                tdi.getConvertorToBytes(), Comparator.naturalOrder());
        final MemDirectory directory = new MemDirectory();
        FileWriter fileWriter = directory.getFileWriter("duck");
        diffWriter.write(fileWriter, 1, true);

        assertThrows(IllegalArgumentException.class,
                () -> diffWriter.write(fileWriter, -1));
    }

}
