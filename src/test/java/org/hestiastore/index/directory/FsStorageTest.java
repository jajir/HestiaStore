package org.hestiastore.index.directory;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.MutableBytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FsStorageTest {

    private static final String FILE_NAME = "pok.txt";

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
    void test_seek() {
        final Directory dir = new FsDirectory(tempDir);

        // Write data
        try (FileWriter fw = dir.getFileWriter(FILE_NAME)) {
            fw.write(TEXT_LONG);
        }

        // Read data and verify seek operation
        try (FileReaderSeekable fr = dir.getFileReaderSeekable(FILE_NAME)) {
            fr.seek(54);
            assertEquals("object", readStr(fr, 6));

            // seek back and verify data
            fr.seek(10);
            assertEquals("stores", readStr(fr, 6));
        }

    }

    private String readStr(final FileReaderSeekable fr, final int length) {
        final MutableBytes buffer = MutableBytes.allocate(length);
        final int read = fr.read(buffer);
        return new String(buffer.array(), 0, read);
    }

}
