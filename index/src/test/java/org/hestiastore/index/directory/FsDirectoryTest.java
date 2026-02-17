package org.hestiastore.index.directory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;

import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FsDirectoryTest {

    @TempDir
    private File tempDir;

    private FsDirectory directory;

    @BeforeEach
    void setUp() {
        directory = new FsDirectory(tempDir);
    }

    @AfterEach
    void tearDown() {
        directory = null;
    }

    @Test
    void test_open_subdirectory_creates_when_missing() {
        final Directory subDirectory = directory.openSubDirectory("child");
        assertTrue(new File(tempDir, "child").isDirectory());
        try (FileWriter fw = subDirectory.getFileWriter("data")) {
            fw.write("x".getBytes());
        }
        assertTrue(new File(tempDir, "child/data").isFile());
    }

    @Test
    void test_subdirectory_mkdir_rmdir_flow() {
        assertTrue(directory.mkdir("sub"));
        assertFalse(directory.mkdir("sub"));

        final Directory subDirectory = directory.openSubDirectory("sub");
        try (FileWriter fw = subDirectory.getFileWriter("data")) {
            fw.write("x".getBytes());
        }

        assertThrows(IndexException.class, () -> directory.rmdir("sub"));

        subDirectory.deleteFile("data");
        assertTrue(directory.rmdir("sub"));
        assertFalse(directory.rmdir("sub"));
    }

    @Test
    void test_open_subdirectory_rejects_file_conflict() {
        try (FileWriter fw = directory.getFileWriter("sub")) {
            fw.write("x".getBytes());
        }

        assertThrows(IndexException.class,
                () -> directory.openSubDirectory("sub"));
    }
}
