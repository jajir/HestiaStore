package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorLong;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class IndexConfigurationManagerTest {

    private final static TypeDescriptor<String> TD_STRING = new TypeDescriptorString();
    private final static TypeDescriptor<Long> TD_LONG = new TypeDescriptorLong();
    private Directory directory;
    private IndexConfiguratonStorage<Long, String> storage;
    private IndexConfigurationManager<Long, String> manager;

    @Test
    void test_key_class_missing() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withValueClass(String.class)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Key class wasn't specified", ex.getMessage());
    }

    @Test
    void test_key_and_value_class_missing() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Key class wasn't specified", ex.getMessage());
    }

    @Test
    void test_maxNumberOfKeysInSegment_is_3() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withMaxNumberOfKeysInSegment(3)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(100) //
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Max number of keys in segment must be at least 4.",
                ex.getMessage());
    }

    @Test
    void test_custom_conf_disk_reading_cache_size_in_not_1024()
            throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withDiskIoBufferSizeInBytes(1000)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(20)//
                .withName("test_index")//
                .withCustomConf()//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals(
                "Parameter 'diskIoBufferSize' vith value '1000' "
                        + "can't be divided by 1024 without reminder",
                ex.getMessage());
    }

    @Test
    void test_maxNumberOfSegmentsInCache_is_1() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class) //
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withCustomConf()//
                .withMaxNumberOfKeysInSegmentCache(22)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(24) //
                .withMaxNumberOfKeysInSegmentIndexPage(33)//
                .withMaxNumberOfKeysInSegment(44)//
                .withMaxNumberOfKeysInCache(143)//
                .withMaxNumberOfSegmentsInCache(1)//
                .withBloomFilterIndexSizeInBytes(0)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));
        assertEquals("Max number of segments in " + "cache must be at least 2.",
                ex.getMessage());
    }

    @Test
    void test_maxNumberOfKeysInSegmentCacheDuringFlushing_is_2()
            throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class) //
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withCustomConf()//
                .withMaxNumberOfKeysInSegmentCache(11)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(2) //
                .withMaxNumberOfKeysInSegmentIndexPage(33)//
                .withMaxNumberOfKeysInSegment(44)//
                .withMaxNumberOfKeysInCache(55)//
                .withMaxNumberOfSegmentsInCache(66)//
                .withBloomFilterIndexSizeInBytes(0)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));
        assertEquals("Max number of keys in segment cache during"
                + " flushing must be at least 3.", ex.getMessage());
    }

    @Test
    void test_maxNumberOfKeysInSegmentCacheDuringFlushing_is_lower_than_maxNumberOfKeysInSegmentCache()
            throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class) //
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withCustomConf()//
                .withMaxNumberOfKeysInSegmentCache(11)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(5) //
                .withMaxNumberOfKeysInSegmentIndexPage(33)//
                .withMaxNumberOfKeysInSegment(44)//
                .withMaxNumberOfKeysInCache(55)//
                .withMaxNumberOfSegmentsInCache(66)//
                .withBloomFilterIndexSizeInBytes(0)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));
        assertEquals("Max number of keys in segment cache during "
                + "flushing must be greater than max number of "
                + "keys in segment cache.", ex.getMessage());
    }

    @Test
    void test_custom_conf() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class) //
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withName("test_index")//
                .withCustomConf()//
                .withMaxNumberOfKeysInSegmentCache(11)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(22) //
                .withMaxNumberOfKeysInSegmentIndexPage(33)//
                .withMaxNumberOfKeysInSegment(44)//
                .withMaxNumberOfKeysInCache(55)//
                .withMaxNumberOfSegmentsInCache(66)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withBloomFilterIndexSizeInBytes(77)//
                .withBloomFilterNumberOfHashFunctions(88)//
                .build();
        final Index<Long, String> index = Index.<Long, String>create(directory,
                config);
        final IndexConfiguration<Long, String> conf = index.getConfiguration();
        assertEquals(11, conf.getMaxNumberOfKeysInSegmentCache());
        assertEquals(22, conf.getMaxNumberOfKeysInSegmentCacheDuringFlushing());
        assertEquals(33, conf.getMaxNumberOfKeysInSegmentIndexPage());
        assertEquals(55, conf.getMaxNumberOfKeysInCache());
        assertEquals(44, conf.getMaxNumberOfKeysInSegment());
        assertEquals(66, conf.getMaxNumberOfSegmentsInCache());
        assertEquals(1024, conf.getDiskIoBufferSize());
        assertEquals(77, conf.getBloomFilterIndexSizeInBytes());
        assertEquals(88, conf.getBloomFilterNumberOfHashFunctions());
    }

    @BeforeEach
    void setup() {
        directory = new MemDirectory();
        storage = new IndexConfiguratonStorage<>(directory);
        manager = new IndexConfigurationManager<>(storage);
    }

    @AfterEach
    void tearDown() {
        storage = null;
        directory = null;
    }

}
