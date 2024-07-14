package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorLong;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class IndexBuilderTest {

    final Directory directory = new MemDirectory();
    private final TypeDescriptor<Long> tdl = new TypeDescriptorLong();
    private final TypeDescriptor<String> tds = new TypeDescriptorString();

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
                "Key type descriptor is null. "
                        + "Set key type descriptor of key class.",
                ex.getMessage());
    }

    @Test
    void test_key_type_definition_after_key_class() throws Exception {
        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> Index.<Long, String>builder().withDirectory(directory)
                        .withKeyClass(Long.class).withKeyTypeDescriptor(tdl)
                        .build());

        assertEquals("KeyClass was alreade set", ex.getMessage());
    }

    @Test
    void test_key_class_after_key_type_definition() throws Exception {
        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> Index.<Long, String>builder().withDirectory(directory)
                        .withKeyTypeDescriptor(tdl).withKeyClass(Long.class)
                        .build());

        assertEquals("Key type descriptor was alreade set. "
                + "Just one should be defined.", ex.getMessage());
    }

    @Test
    void test_missing_directory() throws Exception {
        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> Index.<Long, String>builder().withKeyClass(Long.class)
                        .withValueClass(String.class).build());

        assertEquals("Directory was no spicified.", ex.getMessage());
    }

    @Test
    void test_missing_value_type_definition() throws Exception {
        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> Index.<Long, String>builder().withDirectory(directory)
                        .withKeyClass(Long.class).build());

        assertEquals(
                "Value type descriptor is null. "
                        + "Set value type descriptor of value class.",
                ex.getMessage());
    }

    @Test
    void test_value_type_definition_after_value_class() throws Exception {
        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> Index.<Long, String>builder().withDirectory(directory)
                        .withValueClass(String.class)
                        .withValueTypeDescriptor(tds).build());

        assertEquals("ValueClass was alreade set", ex.getMessage());
    }

    @Test
    void test_value_class_after_value_type_definition() throws Exception {
        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> Index.<Long, String>builder().withDirectory(directory)
                        .withValueTypeDescriptor(tds)
                        .withValueClass(String.class).build());

        assertEquals("Value type descriptor was alreade set. "
                + "Just one should be defined.", ex.getMessage());
    }

}
