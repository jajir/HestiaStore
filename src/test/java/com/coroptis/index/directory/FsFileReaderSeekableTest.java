package com.coroptis.index.directory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class FsFileReaderSeekableTest {

    private final static String FILE_NAME = "pok.txt";

    private final static String TEXT = "Ahoj lidi!";

    private final static byte[] TEXT_LONG = ("This code stores a reference to an "
            + "externally mutable object into the internal "
            + "representation of the object.  If instances are accessed "
            + "by untrusted code, and unchecked changes to the mutable "
            + "object would compromise security or other important "
            + "properties, you will need to do something different. "
            + "Storing a copy of the object is better approach in many "
            + "situations.").getBytes();

    @TempDir
    protected File tempDir;

    @Test
    public void test_read_write_text_fs() throws Exception {
        Directory dir = new FsDirectory(tempDir);
        test_read_write_text(dir);
    }

    @Test
    public void test_read_write_text_mem() throws Exception {
        Directory dir = new MemDirectory();
        test_read_write_text(dir);
    }

    @Test
    public void test_read_write_end_of_file_reached_mem() throws Exception {
        Directory dir = new MemDirectory();
        test_read_long_bytes(dir);
    }

    @Test
    public void test_read_write_end_of_file_reached_fs() throws Exception {
        Directory dir = new FsDirectory(tempDir);
        test_read_long_bytes(dir);
    }

    @Test
    public void test_overwrite_data_fs() throws Exception {
        Directory dir = new FsDirectory(tempDir);
        test_overwrite_file(dir);
    }

    @Test
    public void test_overwrite_data_mem() throws Exception {
        Directory dir = new MemDirectory();
        test_overwrite_file(dir);
    }

    @Test
    public void test_create_empty_file_fs() throws Exception {
        Directory dir = new FsDirectory(tempDir);
        test_create_empty_file_file(dir);
    }

    @Test
    public void test_create_empty_file_mem() throws Exception {
        Directory dir = new MemDirectory();
        test_create_empty_file_file(dir);
    }

    private void test_overwrite_file(final Directory dir) {
        // Write data
        try (FileWriter fw = dir.getFileWriter(FILE_NAME)) {
            fw.write(TEXT.getBytes());
        }

        // write empty file
        try (FileWriter fw = dir.getFileWriter(FILE_NAME)) {
        }

        // assert no data are read
        try (FileReader fr = dir.getFileReader(FILE_NAME)) {
            byte[] bytes = new byte[TEXT_LONG.length];

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
        }

        // assert no data are read, but file exists
        assertTrue(dir.isFileExists(FILE_NAME));
        try (FileReader fr = dir.getFileReader(FILE_NAME)) {
            byte[] bytes = new byte[TEXT_LONG.length];

            final int loadedBytes = fr.read(bytes);
            assertEquals(-1, loadedBytes);
        }
    }

    private void test_read_long_bytes(final Directory dir) {
        try (FileWriter fw = dir.getFileWriter(FILE_NAME)) {
            fw.write(TEXT.getBytes());
        }

        try (FileReader fr = dir.getFileReader(FILE_NAME)) {
            byte[] bytes = new byte[TEXT_LONG.length];

            final int loadedBytes = fr.read(bytes);
            assertEquals(10, loadedBytes);
        }
    }

    private void test_read_write_text(final Directory dir) {
        try (FileWriter fw = dir.getFileWriter(FILE_NAME)) {
            fw.write(TEXT.getBytes());
        }

        try (FileReader fr = dir.getFileReader(FILE_NAME)) {
            byte[] bytes = new byte[TEXT.getBytes().length];
            final int loadedBytes = fr.read(bytes);

            String pok = new String(bytes);
            assertEquals(TEXT, pok);
            assertEquals(TEXT.getBytes().length, loadedBytes);
        }

    }

}
