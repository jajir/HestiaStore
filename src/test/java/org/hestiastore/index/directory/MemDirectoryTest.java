package org.hestiastore.index.directory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.Directory.Access;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MemDirectoryTest {

    private static final byte[] NAME = "Karel".getBytes();
    private static final byte[] SURNAME = "Novotny".getBytes();
    private static final byte[] TEXT = ("This code stores a reference to an "
            + "externally mutable object into the internal "
            + "representation of the object.  If instances are accessed "
            + "by untrusted code, and unchecked changes to the mutable "
            + "object would compromise security or other important "
            + "properties, you will need to do something different. "
            + "Storing a copy of the object is better approach in many "
            + "situations.").getBytes();

    private MemDirectory directory;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
    }

    @AfterEach
    void tearDown() {
        directory = null;
    }

    @Test
    void test_write_and_append() {
        FileWriter fw = directory.getFileWriter("pok");
        fw.write(NAME);
        fw.close();

        fw = directory.getFileWriter("pok", Access.APPEND);
        fw.write(SURNAME);
        fw.close();

        final FileReader fr = directory.getFileReader("pok");
        byte[] read = new byte[NAME.length + SURNAME.length];
        fr.read(read);

        assertEquals("KarelNovotny", new String(read));
    }

    @Test
    void test_write_and_overwrite() {
        FileWriter fw = directory.getFileWriter("pok");
        fw.write(NAME);
        fw.close();

        fw = directory.getFileWriter("pok", Access.OVERWRITE);
        fw.write(SURNAME);
        fw.close();

        final FileReader fr = directory.getFileReader("pok");
        byte[] read = new byte[SURNAME.length];
        fr.read(read);

        assertEquals("Novotny", new String(read));
    }

    @Test
    void test_fileExists() {
        FileWriter fw = directory.getFileWriter("pok");
        fw.write(NAME);
        fw.close();

        assertTrue(directory.isFileExists("pok"));
        assertFalse(directory.isFileExists("anotherOne"));
    }

    @Test
    void test_fileReader_skip() {
        FileWriter fw = directory.getFileWriter("pok");
        fw.write(TEXT);
        fw.close();

        final FileReader fr = directory.getFileReader("pok");

        // verify first skip to correct place
        fr.skip(5);
        assertEquals("code", readStr(fr, 4));

        // verify second skip to correct place
        fr.skip(10);
        assertEquals("reference", readStr(fr, 9));
    }

    @Test
    void test_fileReaderSeakable_seek() {
        FileWriter fw = directory.getFileWriter("pok");
        fw.write(TEXT);
        fw.close();

        final FileReaderSeekable fr = directory.getFileReaderSeekable("pok");

        // verify first skip to correct place
        fr.seek(5);
        assertEquals("code", readStr(fr, 4));

        // verify second skip to correct place
        fr.seek(19);
        assertEquals("reference", readStr(fr, 9));
    }

    @Test
    void test_getFileWriter_invalid_cacheSize() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> directory.getFileWriter("pok", Access.OVERWRITE, 0));

        assertEquals("Buffer size must be greater than zero.", e.getMessage());

    }

    @Test
    void test_subdirectory_mkdir_rmdir_flow() {
        assertTrue(directory.mkdir("sub"));
        assertFalse(directory.mkdir("sub"));

        final Directory subDirectory = directory.openSubDirectory("sub");
        FileWriter fw = subDirectory.getFileWriter("data");
        fw.write(NAME);
        fw.close();

        assertThrows(IndexException.class, () -> directory.rmdir("sub"));

        subDirectory.deleteFile("data");
        assertTrue(directory.rmdir("sub"));
        assertFalse(directory.rmdir("sub"));
    }

    @Test
    void test_subdirectory_rejects_file_name_conflict() {
        FileWriter fw = directory.getFileWriter("sub");
        fw.write(NAME);
        fw.close();

        assertThrows(IndexException.class,
                () -> directory.openSubDirectory("sub"));
    }

    private String readStr(final FileReader fr, final int length) {
        final byte[] bytes = new byte[length];
        fr.read(bytes);
        return new String(bytes);
    }

}
