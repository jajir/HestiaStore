package com.coroptis.index.directory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;

import org.junit.jupiter.api.Test;

public class FsStorageTest {

    private final static String FILE_NAME = "pok.txt";

    private final static String TEXT = "Ahoj lidi!";

    private final static String TEXT_LONG = "Ahoj vsichni lidi!";

    @Test
    public void test_read_write_text_fs() throws Exception {
        Directory dir = new FsDirectory(new File("./target/pok"));
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
        Directory dir = new FsDirectory(new File("./target/pok"));
        test_read_long_bytes(dir);
    }

    @Test
    public void test_overwrite_data_fs() throws Exception {
        Directory dir = new FsDirectory(new File("./target/pok"));
        test_overwrite_file(dir);
    }

    @Test
    public void test_overwrite_data_mem() throws Exception {
        Directory dir = new MemDirectory();
        test_overwrite_file(dir);
    }

    @Test
    public void test_create_empty_file_fs() throws Exception {
        Directory dir = new FsDirectory(new File("./target/pok"));
        test_create_empty_file_file(dir);
    }

    @Test
    public void test_create_empty_file_mem() throws Exception {
        Directory dir = new MemDirectory();
        test_create_empty_file_file(dir);
    }

    private void test_overwrite_file(final Directory dir) {
        // Write data
        try (final FileWriter fw = dir.getFileWriter(FILE_NAME)) {
            fw.write(TEXT.getBytes());
        }

        // write empty file
        try (final FileWriter fw = dir.getFileWriter(FILE_NAME)) {
        }

        // assert no data are read
        try (final FileReader fr = dir.getFileReader(FILE_NAME)) {
            byte[] bytes = new byte[TEXT_LONG.getBytes().length];

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
        try (final FileWriter fw = dir.getFileWriter(FILE_NAME)) {
        }

        // assert no data are read, but file exists
        assertTrue(dir.isFileExists(FILE_NAME));
        try (final FileReader fr = dir.getFileReader(FILE_NAME)) {
            byte[] bytes = new byte[TEXT_LONG.getBytes().length];

            final int loadedBytes = fr.read(bytes);
            assertEquals(-1, loadedBytes);
        }
    }

    private void test_read_long_bytes(final Directory dir) {
        try (final FileWriter fw = dir.getFileWriter(FILE_NAME)) {
            fw.write(TEXT.getBytes());
        }

        try (final FileReader fr = dir.getFileReader(FILE_NAME)) {
            byte[] bytes = new byte[TEXT_LONG.getBytes().length];

            final int loadedBytes = fr.read(bytes);
            assertEquals(10, loadedBytes);
        }
    }

    private void test_read_write_text(final Directory dir) {
        try (final FileWriter fw = dir.getFileWriter(FILE_NAME)) {
            fw.write(TEXT.getBytes());
        }

        try (final FileReader fr = dir.getFileReader(FILE_NAME)) {
            byte[] bytes = new byte[TEXT.getBytes().length];
            final int loadedBytes = fr.read(bytes);

            String pok = new String(bytes);
            assertEquals(TEXT, pok);
            assertEquals(TEXT.getBytes().length, loadedBytes);
        }

    }

}
