package com.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.hestiastore.index.Pair;
import com.hestiastore.index.datatype.TypeDescriptorByte;
import com.hestiastore.index.datatype.TypeDescriptorString;
import com.hestiastore.index.directory.Directory;
import com.hestiastore.index.directory.FileWriter;
import com.hestiastore.index.directory.MemDirectory;

public class IntegrationSortedDataFileWriterTest {

    private final static int DISK_IO_BUFFER_SIZE = 1024;
    private final static String FILE_NAME = "pok.dat";
    private final TypeDescriptorByte byteTd = new TypeDescriptorByte();
    private final TypeDescriptorString stringTd = new TypeDescriptorString();

    @Test
    public void read_incorrect_insert_order_mem() throws Exception {
        final Directory directory = new MemDirectory();
        final FileWriter fileWriter = directory.getFileWriter(FILE_NAME,
                Directory.Access.OVERWRITE, DISK_IO_BUFFER_SIZE);
        try (SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(
                byteTd.getTypeWriter(), fileWriter, stringTd)) {
            assertEquals(0,
                    siw.writeFull(new Pair<String, Byte>("aaabbb", (byte) 1)));
            assertThrows(IllegalArgumentException.class, () -> {
                siw.write(new Pair<String, Byte>("aaa", (byte) 0));
            });
        }
    }

    @Test
    public void test_invalidOrder() throws Exception {
        final Directory directory = new MemDirectory();
        final FileWriter fileWriter = directory.getFileWriter(FILE_NAME,
                Directory.Access.OVERWRITE, DISK_IO_BUFFER_SIZE);
        try (SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(
                byteTd.getTypeWriter(), fileWriter, stringTd)) {
            siw.write(new Pair<String, Byte>("aaa", (byte) 0));
            siw.write(new Pair<String, Byte>("abbb", (byte) 1));
            assertThrows(IllegalArgumentException.class,
                    () -> siw.write(new Pair<String, Byte>("aaaa", (byte) 2)));
        }
    }

    @Test
    public void test_duplicatedValue() throws Exception {
        final Directory directory = new MemDirectory();
        final FileWriter fileWriter = directory.getFileWriter(FILE_NAME,
                Directory.Access.OVERWRITE, DISK_IO_BUFFER_SIZE);
        try (SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(
                byteTd.getTypeWriter(), fileWriter, stringTd)) {
            siw.write(new Pair<String, Byte>("aaa", (byte) 0));
            siw.write(new Pair<String, Byte>("abbb", (byte) 1));
            assertThrows(IllegalArgumentException.class,
                    () -> siw.write(new Pair<String, Byte>("abbb", (byte) 2)));
        }
    }

    @Test
    public void test_null_key() throws Exception {
        final Directory directory = new MemDirectory();
        final FileWriter fileWriter = directory.getFileWriter(FILE_NAME,
                Directory.Access.OVERWRITE, DISK_IO_BUFFER_SIZE);
        try (SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(
                byteTd.getTypeWriter(), fileWriter, stringTd)) {
            assertThrows(NullPointerException.class,
                    () -> siw.write(new Pair<String, Byte>(null, (byte) 0)));
        }

    }

}
