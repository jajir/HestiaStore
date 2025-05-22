package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorLong;
import com.coroptis.index.datatype.TypeDescriptorString;

@ExtendWith(MockitoExtension.class)
public class IndexConfigurationManagerTest {

    private final static TypeDescriptor<String> TD_STRING = new TypeDescriptorString();
    private final static TypeDescriptor<Long> TD_LONG = new TypeDescriptorLong();
    private final static IndexConfiguration<Long, String> CONFIG = IndexConfiguration
            .<Long, String>builder()//
            .withKeyClass(Long.class) //
            .withValueClass(String.class)//
            .withKeyTypeDescriptor(TD_LONG)//
            .withValueTypeDescriptor(TD_STRING)//
            .withName("test_index")//
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
    private IndexConfigurationBuilder<Long, String> configBuilder;

    @Mock
    private IndexConfiguratonStorage<Long, String> storage;

    private IndexConfigurationManager<Long, String> manager;

    @Test
    void test_save_key_class_missing() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withValueClass(String.class)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Key class wasn't specified", ex.getMessage());
    }

    @Test
    void test_save_value_class_missing() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Value class wasn't specified", ex.getMessage());
    }

    @Test
    void test_save_key_type_descriptor_missing() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class)//
                .withValueClass(String.class)//
                .withValueTypeDescriptor(TD_STRING)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Key type descriptor is null.", ex.getMessage());
    }

    @Test
    void test_save_value_type_descriptor_missing() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_LONG)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Value type descriptor is null.", ex.getMessage());
    }

    @Test
    void test_save_maxNumberOfKeysInCache_is_less_than_3() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withMaxNumberOfKeysInCache(2)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Max number of keys in cache must be at least 3.",
                ex.getMessage());
    }

    @Test
    void test_save_maxNumberOfKeysInSegment_is_less_than_4() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withMaxNumberOfKeysInCache(3)//
                .withMaxNumberOfKeysInSegment(3)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Max number of keys in segment must be at least 4.",
                ex.getMessage());
    }

    @Test
    void test_save_maxNumberOfSegmentsInCache_is_less_than_3()
            throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class) //
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withMaxNumberOfKeysInCache(3)//
                .withMaxNumberOfKeysInSegment(4)//
                .withMaxNumberOfSegmentsInCache(1)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));
        assertEquals("Max number of segments in " + "cache must be at least 2.",
                ex.getMessage());
    }

    @Test
    void test_save_maxNumberOfKeysInSegmentCacheDuringFlushing_is_less_than_3()
            throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class) //
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withMaxNumberOfKeysInCache(3)//
                .withMaxNumberOfKeysInSegment(4)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(2) //
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));
        assertEquals("Max number of keys in segment cache during"
                + " flushing must be at least 3.", ex.getMessage());
    }

    @Test
    void test_save_maxNumberOfKeysInSegmentCacheDuringFlushing_is_lower_than_maxNumberOfKeysInSegmentCache()
            throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class) //
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withMaxNumberOfKeysInCache(3)//
                .withMaxNumberOfKeysInSegment(4)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withMaxNumberOfKeysInSegmentCache(11)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(5) //
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));
        assertEquals("Max number of keys in segment cache during "
                + "flushing must be greater than max number of "
                + "keys in segment cache.", ex.getMessage());
    }

    @Test
    void test_save_custom_conf_disk_reading_cache_size_in_not_1024()
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
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals(
                "Parameter 'diskIoBufferSize' vith value '1000' "
                        + "can't be divided by 1024 without reminder",
                ex.getMessage());
    }

    @Test
    void test_save_index_name_is_empy() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(20)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Index name is null.", ex.getMessage());
    }

    @Test
    void test_save() throws Exception {
        manager.save(CONFIG);

        verify(storage, Mockito.times(1)).save(CONFIG);
    }

    @Test
    void test_mergeWithStored() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class) //
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withName("test_index")//
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

        when(storage.load()).thenReturn(CONFIG);
        final IndexConfiguration<Long, String> ret = manager
                .mergeWithStored(config);
        // verify that unchanged object is not saved
        verify(storage, Mockito.times(0)).save(any());

        assertNotNull(ret);
        // returned object can't be changed
    }

    @Test
    void test_mergeWithStored_indexName() {
        final IndexConfiguration<Long, String> config = configBuilder//
                .withName("pandemonium")//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final IndexConfiguration<Long, String> ret = manager
                .mergeWithStored(config);
        verify(storage, Mockito.times(1)).save(any());
        assertNotNull(ret);

        assertEquals("pandemonium", ret.getIndexName());
    }

    @BeforeEach
    void setup() {
        manager = new IndexConfigurationManager<>(storage);
        configBuilder = IndexConfiguration.<Long, String>builder()//
                .withKeyClass(Long.class) //
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withName("test_index")//
                .withMaxNumberOfKeysInSegmentCache(11)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(22) //
                .withMaxNumberOfKeysInSegmentIndexPage(33)//
                .withMaxNumberOfKeysInSegment(44)//
                .withMaxNumberOfKeysInCache(55)//
                .withMaxNumberOfSegmentsInCache(66)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withBloomFilterIndexSizeInBytes(77)//
                .withBloomFilterNumberOfHashFunctions(88)//
        ;
    }

    @AfterEach
    void tearDown() {
        manager = null;
        configBuilder = null;
    }

}
