package com.coroptis.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Comparator;

import org.junit.jupiter.api.Test;

import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeDescriptorByte;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileWriter;
import com.coroptis.index.directory.MemDirectory;

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
        final DiffKeyWriter<String> diffKeyWriter = new DiffKeyWriter<>(stringTd.getConvertorToBytes(),
                Comparator.naturalOrder());
        try (SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(byteTd.getTypeWriter(),
                fileWriter, diffKeyWriter)) {
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
        final DiffKeyWriter<String> diffKeyWriter = new DiffKeyWriter<>(stringTd.getConvertorToBytes(),
                Comparator.naturalOrder());
        try (SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(byteTd.getTypeWriter(),
                fileWriter, diffKeyWriter)) {
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
        final DiffKeyWriter<String> diffKeyWriter = new DiffKeyWriter<>(stringTd.getConvertorToBytes(),
                Comparator.naturalOrder());
        try (SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(byteTd.getTypeWriter(),
                fileWriter, diffKeyWriter)) {
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
        final DiffKeyWriter<String> diffKeyWriter = new DiffKeyWriter<>(stringTd.getConvertorToBytes(),
                Comparator.naturalOrder());
        try (SortedDataFileWriter<String, Byte> siw = new SortedDataFileWriter<>(byteTd.getTypeWriter(),
                fileWriter, diffKeyWriter)) {
            assertThrows(NullPointerException.class,
                    () -> siw.write(new Pair<String, Byte>(null, (byte) 0)));
        }

    }

}
