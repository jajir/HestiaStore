package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.Pair;
import org.hestiastore.index.datatype.TypeDescriptorByte;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileWriter;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class IntegrationSortedDataFileWriterTest {

    private static final int DISK_IO_BUFFER_SIZE = 1024;
    private static final String FILE_NAME = "pok.dat";
    private final TypeDescriptorByte byteTd = new TypeDescriptorByte();
    private final TypeDescriptorString stringTd = new TypeDescriptorString();

    @Test
    public void read_incorrect_insert_order_mem() {
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
    void test_invalidOrder() {
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
    void test_duplicatedValue() {
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
    void test_null_key() {
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
