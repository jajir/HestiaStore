package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SortedDataFileTest {

    private static final String FILE_NAME = "test.sdf";
    private static final TypeDescriptorShortString STD = new TypeDescriptorShortString();
    private static final TypeDescriptorInteger LTD = new TypeDescriptorInteger();
    private static final int IO_BUFFER_SIZE = 1024 * 4;

    @Mock
    private Directory directory;

    @Test
    void test_constructor_missing_directory() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> SortedDataFile.fromDirectory(null, FILE_NAME, STD,
                        LTD, IO_BUFFER_SIZE));

        assertEquals("Property 'directoryFacade' must not be null.",
                err.getMessage());
    }

    @Test
    void test_constructor_missing_fileName() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> SortedDataFile.fromDirectory(
                        directory,
                        null, STD, LTD, IO_BUFFER_SIZE));

        assertEquals("Property 'fileName' must not be null.", err.getMessage());
    }

    @Test
    void test_constructor_missing_keyTypeDescriptor() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> SortedDataFile.fromDirectory(
                        directory,
                        FILE_NAME, null, LTD, IO_BUFFER_SIZE));

        assertEquals("Property 'keyTypeDescriptor' must not be null.",
                err.getMessage());
    }

    @Test
    void test_constructor_missing_valueTypeDescriptor() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> SortedDataFile.fromDirectory(
                        directory,
                        FILE_NAME, STD, null, IO_BUFFER_SIZE));

        assertEquals("Property 'valueTypeDescriptor' must not be null.",
                err.getMessage());
    }

    @Test
    void test_constructor_zero_IOBuffer() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> SortedDataFile.fromDirectory(
                        directory,
                        FILE_NAME, STD, LTD, 0));

        assertEquals("Property 'ioBufferSize' must be greater than 0",
                err.getMessage());
    }

    @Test
    void test_constructor() {
        final SortedDataFile<String, Integer> sortedDataFile = SortedDataFile
                .fromDirectory(
                        directory,
                        FILE_NAME, STD, LTD, IO_BUFFER_SIZE);

        assertNotNull(sortedDataFile);
    }

}
