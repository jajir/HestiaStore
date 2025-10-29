package org.hestiastore.index.directory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.MutableBytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FsFileReaderSeekableTest {

    private static final String FILE_NAME = "pok.txt";

    private static final String TEXT = "Ahoj lidi!";

    private static final Bytes TEXT_BYTES = Bytes.of(TEXT.getBytes());

    private static final Bytes TEXT_LONG = Bytes
            .of(("This code stores a reference to an "
                    + "externally mutable object into the internal "
                    + "representation of the object.  If instances are accessed "
                    + "by untrusted code, and unchecked changes to the mutable "
                    + "object would compromise security or other important "
                    + "properties, you will need to do something different. "
                    + "Storing a copy of the object is better approach in many "
                    + "situations.").getBytes());

    @TempDir
    protected File tempDir;

    @Test
    void test_read_write_text_fs() {
        Directory dir = new FsDirectory(tempDir);
        test_read_write_text(dir);
    }

    @Test
    void test_read_write_text_mem() {
        Directory dir = new MemDirectory();
        test_read_write_text(dir);
    }

    @Test
    void test_read_write_end_of_file_reached_mem() {
        Directory dir = new MemDirectory();
        test_read_long_bytes(dir);
    }

    @Test
    void test_read_write_end_of_file_reached_fs() {
        Directory dir = new FsDirectory(tempDir);
        test_read_long_bytes(dir);
    }

    @Test
    void test_overwrite_data_fs() {
        Directory dir = new FsDirectory(tempDir);
        test_overwrite_file(dir);
    }

    @Test
    void test_overwrite_data_mem() {
        Directory dir = new MemDirectory();
        test_overwrite_file(dir);
    }

    @Test
    void test_create_empty_file_fs() {
        Directory dir = new FsDirectory(tempDir);
        test_create_empty_file_file(dir);
    }

    @Test
    void test_create_empty_file_mem() {
        Directory dir = new MemDirectory();
        test_create_empty_file_file(dir);
    }

    private void test_overwrite_file(final Directory dir) {
        // Write data
        try (FileWriter fw = dir.getFileWriter(FILE_NAME)) {
            fw.write(TEXT_BYTES);
        }

        // write empty file
        try (FileWriter fw = dir.getFileWriter(FILE_NAME)) {
            // intentionally empty
        }

        // assert no data are read
        try (FileReader fr = dir.getFileReader(FILE_NAME)) {
            final MutableBytes bytes = MutableBytes
                    .allocate(TEXT_LONG.length());

            final int loadedBytes = fr.read(bytes);
            assertEquals(-1, loadedBytes);
        }
    }

    private void test_create_empty_file_file(final Directory dir) {
        // optionally delete file
        if (dir.isFileExists(FILE_NAME)) {
            dir.deleteFile(FILE_NAME);
        }
        // write empty file
        try (FileWriter fw = dir.getFileWriter(FILE_NAME)) {
            // intentionally empty
        }

        // assert no data are read, but file exists
        assertTrue(dir.isFileExists(FILE_NAME));
        try (FileReader fr = dir.getFileReader(FILE_NAME)) {
            final MutableBytes bytes = MutableBytes
                    .allocate(TEXT_LONG.length());

            final int loadedBytes = fr.read(bytes);
            assertEquals(-1, loadedBytes);
        }
    }

    private void test_read_long_bytes(final Directory dir) {
        try (FileWriter fw = dir.getFileWriter(FILE_NAME)) {
            fw.write(TEXT_BYTES);
        }

        try (FileReader fr = dir.getFileReader(FILE_NAME)) {
            final MutableBytes bytes = MutableBytes
                    .allocate(TEXT_LONG.length());

            final int loadedBytes = fr.read(bytes);
            assertEquals(10, loadedBytes);
        }
    }

    private void test_read_write_text(final Directory dir) {
        try (FileWriter fw = dir.getFileWriter(FILE_NAME)) {
            fw.write(TEXT_BYTES);
        }

        try (FileReader fr = dir.getFileReader(FILE_NAME)) {
            final byte[] source = TEXT.getBytes();
            final MutableBytes bytes = MutableBytes.allocate(source.length);
            final int loadedBytes = fr.read(bytes);

            String pok = new String(bytes.array(), 0, loadedBytes);
            assertEquals(TEXT, pok);
            assertEquals(source.length, loadedBytes);
        }

    }

}
