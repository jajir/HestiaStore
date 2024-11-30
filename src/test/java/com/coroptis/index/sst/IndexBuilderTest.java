package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
    void test_key_class_missing() throws Exception {
        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> Index.<Long, String>builder()//
                        .withDirectory(directory)//
                        .withValueClass(String.class)//
                        .build());

        assertEquals("Key class wasn't specified", ex.getMessage());
    }

    @Test
    void test_key_and_value_class_missing() throws Exception {
        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> Index.<Long, String>builder()//
                        .withDirectory(directory)//
                        .build());

        assertEquals("Key class wasn't specified", ex.getMessage());
    }

    @Test
    void test_key_class_with_missing_type_definition() throws Exception {
        final Exception ex = assertThrows(IllegalStateException.class,
                () -> Index.<Byte, String>builder()//
                        .withDirectory(directory)//
                        .withKeyClass(Byte.class)//
                        .withValueClass(String.class)//
                        .build());

        assertEquals("There is not data type descriptor in registry "
                + "for class 'class java.lang.Byte'", ex.getMessage());
    }

    @Test
    void test_class_and_data_type_definitions_filled() throws Exception {
        final Index<Long, String> index = Index.<Long, String>builder()//
                .withDirectory(directory)//
                .withKeyClass(Long.class)//
                .withKeyTypeDescriptor(tdl)//
                .withValueClass(String.class)//
                .withValueTypeDescriptor(tds)//
                .build();

        assertNotNull(index);
    }

    @Test
    void test_directory_missing() throws Exception {
        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> Index.<Long, String>builder()//
                        .withKeyClass(Long.class)//
                        .withValueClass(String.class)//
                        .build());

        assertEquals("Directory was no specified.", ex.getMessage());
    }

    @Test
    void test_value_class_missing_type_definition() throws Exception {
        final Exception ex = assertThrows(IllegalStateException.class,
                () -> Index.<Long, Byte>builder()//
                        .withDirectory(directory)//
                        .withKeyClass(Long.class)//
                        .withValueClass(Byte.class)//
                        .build());

        assertEquals("There is not data type descriptor in registry"
                + " for class 'class java.lang.Byte'", ex.getMessage());
    }

    @Test
    void test_MaxNumberOfKeysInSegment_is_3() throws Exception {
        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> Index.<Long, String>builder()//
                        .withDirectory(directory)//
                        .withKeyClass(Long.class)//
                        .withValueClass(String.class)//
                        .withMaxNumberOfKeysInSegment(3)//
                        .build());

        assertEquals("Max number of keys in segment must be at least 4.", ex.getMessage());
    }

    @Test
    void test_conf_missing_specific_type_configuration() throws Exception {
        final Exception ex = assertThrows(IllegalStateException.class,
                () -> Index.<Long, String>builder()//
                .withDirectory(directory)//
                        .withKeyClass(Long.class)//
                        .withValueClass(String.class) //
                        .withConf("11.5G")//
                        .build());

        assertEquals(
                "Configuration for key class 'java.lang.Long' "
                        + "and memory configuration '11.5G' was not specified.",
                ex.getMessage());
    }

    @Test
    void test_conf_default_type_configuration() throws Exception {
        final Index<Long, String> index = Index.<Long, String>builder()
                .withDirectory(directory).withKeyClass(Long.class)
                .withValueClass(String.class).build();
        final SsstIndexConf conf = getConf(index);
        assertEquals(500_000L, conf.getMaxNumberOfKeysInSegmentCache());
        assertEquals(10_000_000,
                conf.getMaxNumberOfKeysInSegmentCacheDuringFlushing());
        assertEquals(1_000, conf.getMaxNumberOfKeysInSegmentIndexPage());
        assertEquals(5_000_000, conf.getMaxNumberOfKeysInCache());
        assertEquals(10_000_000, conf.getMaxNumberOfKeysInSegment());
        assertEquals(10, conf.getMaxNumberOfSegmentsInCache());
        assertEquals(1_048_576, conf.getFileReadingBufferSizeInBytes());
        assertEquals(100_000, conf.getBloomFilterIndexSizeInBytes());
        assertEquals(2, conf.getBloomFilterNumberOfHashFunctions());
    }

    @Test
    void test_custom_conf_disk_reading_cache_size_in_not_1024()
            throws Exception {
        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> Index.<Long, String>builder()//
                        .withDirectory(directory)//
                        .withKeyClass(Long.class)//
                        .withValueClass(String.class)//
                        .withFileReadingBufferSizeInBytes(1000)//
                        .withCustomConf()//
                        .build());

        assertEquals(
                "Parameter 'indexBufferSizeInBytes' vith value '1000' "
                        + "can't be divided by 1024 without reminder",
                ex.getMessage());
    }

    @Test
    void test_custom_conf() throws Exception {
        final Index<Long, String> index = Index.<Long, String>builder() //
                .withDirectory(directory) //
                .withKeyClass(Long.class) //
                .withValueClass(String.class)//
                .withCustomConf()//
                .withMaxNumberOfKeysInSegmentCache(11)//
                .setMaxNumberOfKeysInSegmentCacheDuringFlushing(22) //
                .withMaxNumberOfKeysInSegmentIndexPage(33)//
                .withMaxNumberOfKeysInSegment(44)//
                .withMaxNumberOfKeysInCache(55)//
                .setMaxNumberOfSegmentsInCache(66)//
                .withFileReadingBufferSizeInBytes(1024)//
                .withBloomFilterIndexSizeInBytes(77)//
                .withBloomFilterNumberOfHashFunctions(88)//
                .build();
        final SsstIndexConf conf = getConf(index);
        assertEquals(11, conf.getMaxNumberOfKeysInSegmentCache());
        assertEquals(22, conf.getMaxNumberOfKeysInSegmentCacheDuringFlushing());
        assertEquals(33, conf.getMaxNumberOfKeysInSegmentIndexPage());
        assertEquals(55, conf.getMaxNumberOfKeysInCache());
        assertEquals(44, conf.getMaxNumberOfKeysInSegment());
        assertEquals(66, conf.getMaxNumberOfSegmentsInCache());
        assertEquals(1024, conf.getFileReadingBufferSizeInBytes());
        assertEquals(77, conf.getBloomFilterIndexSizeInBytes());
        assertEquals(88, conf.getBloomFilterNumberOfHashFunctions());
    }

    private <M, N> SsstIndexConf getConf(Index<M, N> index) {
        final SstIndexImpl<M, N> ind = (SstIndexImpl<M, N>) index;
        return ind.getConf();
    }

}
