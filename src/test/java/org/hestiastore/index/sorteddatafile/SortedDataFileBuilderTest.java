package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.Test;

class SortedDataFileBuilderTest {

    private static final String FILE_NAME = "data.sdf";

    @Test
    void test_build_missing_directory() {
        final SortedDataFileBuilder<String, Integer> builder = new SortedDataFileBuilder<>();
        final Exception err = assertThrows(IllegalStateException.class,
                builder::build);

        assertEquals("Directory must be provided", err.getMessage());
    }

    @Test
    void test_build_missing_file_name() {
        try (AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(new MemDirectory())) {
            final SortedDataFileBuilder<String, Integer> builder = new SortedDataFileBuilder<String, Integer>()
                    .withAsyncDirectory(asyncDirectory)
                    .withKeyTypeDescriptor(new TypeDescriptorShortString())
                    .withValueTypeDescriptor(new TypeDescriptorInteger());

            final Exception err = assertThrows(IllegalArgumentException.class,
                    builder::build);

            assertEquals("Property 'fileName' must not be null.",
                    err.getMessage());
        }
    }

    @Test
    void test_build_missing_key_type_descriptor() {
        try (AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(new MemDirectory())) {
            final SortedDataFileBuilder<String, Integer> builder = new SortedDataFileBuilder<String, Integer>()
                    .withAsyncDirectory(asyncDirectory)
                    .withFileName(FILE_NAME)
                    .withValueTypeDescriptor(new TypeDescriptorInteger());

            final Exception err = assertThrows(IllegalArgumentException.class,
                    builder::build);

            assertEquals("Property 'keyTypeDescriptor' must not be null.",
                    err.getMessage());
        }
    }

    @Test
    void test_build_missing_value_type_descriptor() {
        try (AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(new MemDirectory())) {
            final SortedDataFileBuilder<String, Integer> builder = new SortedDataFileBuilder<String, Integer>()
                    .withAsyncDirectory(asyncDirectory)
                    .withFileName(FILE_NAME)
                    .withKeyTypeDescriptor(new TypeDescriptorShortString());

            final Exception err = assertThrows(IllegalArgumentException.class,
                    builder::build);

            assertEquals("Property 'valueTypeDescriptor' must not be null.",
                    err.getMessage());
        }
    }

    @Test
    void test_build_invalid_buffer_size() {
        try (AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(new MemDirectory())) {
            final SortedDataFileBuilder<String, Integer> builder = new SortedDataFileBuilder<String, Integer>()
                    .withAsyncDirectory(asyncDirectory)
                    .withFileName(FILE_NAME)
                    .withKeyTypeDescriptor(new TypeDescriptorShortString())
                    .withValueTypeDescriptor(new TypeDescriptorInteger())
                    .withDiskIoBufferSize(0);

            final Exception err = assertThrows(IllegalArgumentException.class,
                    builder::build);

            assertEquals("Property 'ioBufferSize' must be greater than 0",
                    err.getMessage());
        }
    }

    @Test
    void test_build_success() {
        try (AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(new MemDirectory())) {
            final SortedDataFileBuilder<String, Integer> builder = new SortedDataFileBuilder<String, Integer>()
                    .withAsyncDirectory(asyncDirectory)
                    .withFileName(FILE_NAME)
                    .withKeyTypeDescriptor(new TypeDescriptorShortString())
                    .withValueTypeDescriptor(new TypeDescriptorInteger());

            assertNotNull(builder.build());
        }
    }
}
