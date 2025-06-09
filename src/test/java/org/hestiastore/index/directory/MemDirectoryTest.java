package org.hestiastore.index.directory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.directory.Directory.Access;
import org.junit.jupiter.api.Test;

public class MemDirectoryTest {

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

    @Test
    void test_write_and_append() {
        final MemDirectory directory = new MemDirectory();
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
        final MemDirectory directory = new MemDirectory();
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
        final MemDirectory directory = new MemDirectory();
        FileWriter fw = directory.getFileWriter("pok");
        fw.write(NAME);
        fw.close();

        assertTrue(directory.isFileExists("pok"));
        assertFalse(directory.isFileExists("anotherOne"));
    }

    @Test
    void test_fileReader_skip() {
        final MemDirectory directory = new MemDirectory();
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
        final MemDirectory directory = new MemDirectory();
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
        final MemDirectory directory = new MemDirectory();
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> directory.getFileWriter("pok", Access.OVERWRITE, 0));

        assertEquals("Buffer size must be greater than zero.", e.getMessage());

    }

    private String readStr(final FileReader fr, final int length) {
        final byte[] bytes = new byte[length];
        fr.read(bytes);
        return new String(bytes);
    }

}
