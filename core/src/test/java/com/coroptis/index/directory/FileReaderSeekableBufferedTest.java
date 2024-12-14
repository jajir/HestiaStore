package com.coroptis.index.directory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FileReaderSeekableBufferedTest {

    private final static String FILE1 = "pok.txt";

    private final static byte[] TEXT_LONG = ("This code stores a reference to an "
            + "externally mutable object into the internal "
            + "representation of the object.  If instances are accessed "
            + "by untrusted code, and unchecked changes to the mutable "
            + "object would compromise security or other important "
            + "properties, you will need to do something different. "
            + "Storing a copy of the object is better approach in many "
            + "situations.").getBytes();

    private final static byte[] TEXT_SHORT = "This code stores".getBytes();

    private final static byte[] TEXT_TINY = "This".getBytes();

    @Mock
    private FileReaderSeekable fileReaderSeekable;

    @Test
    void test_read_short_text_from_0_from_long_text() throws Exception {
        final FileReaderSeekableBuffered reader = makeReader(TEXT_LONG);

        assertEquals("This", readStr(reader, 4));
    }

    @Test
    void test_read_long_text_from_0_from_long_text() throws Exception {
        final FileReaderSeekableBuffered reader = makeReader(TEXT_LONG);

        assertEquals("This code stores", readStr(reader, 16));
    }

    @Test
    void test_read_short_text_from_0_from_tiny_text() throws Exception {
        final FileReaderSeekableBuffered reader = makeReader(TEXT_TINY);

        assertEquals("This", readStr(reader, 10, 4));
    }

    @Test
    void test_read_long_text_from_0_from_short_text() throws Exception {
        final FileReaderSeekableBuffered reader = makeReader(TEXT_SHORT);

        assertEquals("This code stores", readStr(reader, 20, 16));
    }

    @Test
    void test_read_short_text_from_35_from_long_text() throws Exception {
        final FileReaderSeekableBuffered reader = makeReader(TEXT_LONG);
        reader.seek(35);

        assertEquals("externally", readStr(reader, 10));
    }

    @Test
    void test_read_long_text_from_35_from_long_text() throws Exception {
        final FileReaderSeekableBuffered reader = makeReader(TEXT_LONG);
        reader.seek(35);

        assertEquals("externally mutable", readStr(reader, 18));
    }

    @Test
    void test_multiple_reads_from_long_text() throws Exception {
        final FileReaderSeekableBuffered reader = makeReader(TEXT_LONG);
        reader.seek(35);

        assertEquals("externally mutable ", readStr(reader, 19));
        assertEquals("object into the internal", readStr(reader, 24));

        reader.seek(5);
        assertEquals("code", readStr(reader, 4));
    }

    @Test
    void test_read_short_text_from_20_from_tiny_text() throws Exception {
        final FileReaderSeekableBuffered reader = makeReader(TEXT_TINY);

        assertThrows(IllegalArgumentException.class, () -> reader.seek(20));
    }

    private FileReaderSeekableBuffered makeReader(final byte[] text) {
        final Directory directory = new MemDirectory();
        try (FileWriter fw = directory.getFileWriter(FILE1)) {
            fw.write(text);
        }

        final FileReaderSeekableBuffered reader = new FileReaderSeekableBuffered(
                directory.getFileReaderSeekable(FILE1), 10);
        return reader;
    }

    private String readStr(final FileReaderSeekable fr, final int length,
            final int expectedLength) {
        final byte[] bytes = new byte[length];
        final int readed = fr.read(bytes);
        assertEquals(expectedLength, readed);
        byte[] out = new byte[readed];
        System.arraycopy(bytes, 0, out, 0, readed);
        return new String(out);
    }

    private String readStr(final FileReaderSeekable fr, final int length) {
        return readStr(fr, length, length);
    }

}
