package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class IndexBuilderTest {

    final Directory directory = new MemDirectory();

    @Test
    void test_disk_reading_cache_size_in_not_1024() throws Exception {
        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> Index.<Long, String>builder()
                        .withIndexBufferSizeInBytes(1000).build());

        assertEquals(
                "Parameter 'indexBufferSizeInBytes' vith value '1000' "
                        + "can't be divided by 1024 without reminder",
                ex.getMessage());
    }

    @Test
    void test_missing_key_type_definition() throws Exception {
        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> Index.<Long, String>builder().withDirectory(directory)
                        .withValueClass(String.class).build());

        assertEquals(
                "Parameter 'indexBufferSizeInBytes' vith value '1000' "
                        + "can't be divided by 1024 without reminder",
                ex.getMessage());
    }

    @Test
    void test_missing_value_type_definition() throws Exception {
        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> Index.<Long, String>builder().withDirectory(directory)
                        .withKeyClass(Long.class).build());

        assertEquals(
                "Parameter 'indexBufferSizeInBytes' vith value '1000' "
                        + "can't be divided by 1024 without reminder",
                ex.getMessage());
    }

}
