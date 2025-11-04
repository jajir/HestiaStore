package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorByte;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileWriter;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class IntegrationSortedDataFileWriterTest {

    private static final Entry<String, Byte> P_AAABBB_1 = new Entry<>("aaabbb",
            (byte) 1);
    private static final Entry<String, Byte> P_AAA_0 = new Entry<>("aaa",
            (byte) 0);
    private static final Entry<String, Byte> P_ABBB_1 = new Entry<>("abbb",
            (byte) 1);
    private static final Entry<String, Byte> P_AAAA_2 = new Entry<>("aaaa",
            (byte) 2);
    private static final Entry<String, Byte> P_ABBB_2 = new Entry<>("abbb",
            (byte) 2);
    private static final Entry<String, Byte> P_NULL_0 = new Entry<>(null,
            (byte) 0);
    private static final int DISK_IO_BUFFER_SIZE = 1024;
    private static final String FILE_NAME = "pok.dat";
    private final TypeDescriptorByte byteTd = new TypeDescriptorByte();
    private final TypeDescriptorShortString stringTd = new TypeDescriptorShortString();

    @Test
    void read_incorrect_insert_order_mem() {
        final Directory directory = new MemDirectory();
        final FileWriter fileWriter = directory.getFileWriter(FILE_NAME,
                Directory.Access.OVERWRITE, DISK_IO_BUFFER_SIZE);
        try (SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(
                byteTd.getTypeWriter(), fileWriter, stringTd)) {
            siw.write(P_AAABBB_1);
            assertThrows(IllegalArgumentException.class, () -> {
                siw.write(P_AAA_0);
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
            siw.write(P_AAA_0);
            siw.write(P_ABBB_1);
            assertThrows(IllegalArgumentException.class,
                    () -> siw.write(P_AAAA_2));
        }
    }

    @Test
    void test_duplicatedValue() {
        final Directory directory = new MemDirectory();
        final FileWriter fileWriter = directory.getFileWriter(FILE_NAME,
                Directory.Access.OVERWRITE, DISK_IO_BUFFER_SIZE);
        try (SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(
                byteTd.getTypeWriter(), fileWriter, stringTd)) {
            siw.write(P_AAA_0);
            siw.write(P_ABBB_1);
            assertThrows(IllegalArgumentException.class,
                    () -> siw.write(P_ABBB_2));
        }
    }

    @Test
    void test_null_key() {
        final Directory directory = new MemDirectory();
        final FileWriter fileWriter = directory.getFileWriter(FILE_NAME,
                Directory.Access.OVERWRITE, DISK_IO_BUFFER_SIZE);
        try (SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(
                byteTd.getTypeWriter(), fileWriter, stringTd)) {
            assertThrows(IllegalArgumentException.class,
                    () -> siw.write(P_NULL_0));
        }

    }

}
