package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

import com.coroptis.index.datatype.TypeDescriptorLong;
import com.coroptis.index.datatype.TypeDescriptorString;

@ExtendWith(MockitoExtension.class)
public class IndexConfigurationManagerTest {

    private final static String TD_STRING = TypeDescriptorString.class
            .getSimpleName();
    private final static String TD_LONG = TypeDescriptorLong.class
            .getSimpleName();
    private final static IndexConfiguration<Long, String> CONFIG = IndexConfiguration
            .<Long, String>builder()//
            .withKeyClass(Long.class) //
            .withValueClass(String.class)//
            .withKeyTypeDescriptor(TD_LONG)//
            .withValueTypeDescriptor(TD_STRING)//
            .withName("test_index")//
            .withLogEnabled(false)//
            .withThreadSafe(false)//
            .withMaxNumberOfKeysInSegmentCache(11L)//
            .withMaxNumberOfKeysInSegmentCacheDuringFlushing(22L) //
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
    void test_save_key_class_is_null() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withValueClass(String.class)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Key class wasn't specified", ex.getMessage());
    }

    @Test
    void test_save_value_class_is_null() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Value class wasn't specified", ex.getMessage());
    }

    @Test
    void test_save_index_name_is_null() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class)//
                .withValueClass(String.class)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Index name is null.", ex.getMessage());
    }

    @Test
    void test_save_key_type_descriptor_is_null() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class)//
                .withValueClass(String.class)//
                .withValueTypeDescriptor(TD_STRING)//
                .withName("test_index")//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Key type descriptor is null.", ex.getMessage());
    }

    @Test
    void test_save_thread_safe_is_null() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class)//
                .withValueClass(String.class)//
                .withName("test_index")//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Value of thread safe is null.", ex.getMessage());
    }

    @Test
    void test_save_log_enabled_missing() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class)//
                .withValueClass(String.class)//
                .withName("test_index")//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withThreadSafe(true)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Value of log enable is null.", ex.getMessage());
    }

    @Test
    void test_save_maxNumberOfKeysInCache_is_null() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class)//
                .withValueClass(String.class)//
                .withName("test_index")//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withThreadSafe(true)//
                .withLogEnabled(true)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Property ‘MaxNumberOfKeysInCache’ must not be null.",
                ex.getMessage());
    }

    @Test
    void test_save_maxNumberOfKeysInCache_is_less_than_3() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class)//
                .withValueClass(String.class)//
                .withName("test_index")//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withThreadSafe(true)//
                .withLogEnabled(true)//
                .withMaxNumberOfKeysInCache(2)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Max number of keys in cache must be at least 3.",
                ex.getMessage());
    }

    @Test
    void test_save_maxNumberOfKeysInSegment_is_null() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class)//
                .withValueClass(String.class)//
                .withName("test_index")//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withThreadSafe(true)//
                .withLogEnabled(true)//
                .withMaxNumberOfKeysInCache(3)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Property ‘MaxNumberOfKeysInSegment’ must not be null.",
                ex.getMessage());
    }

    @Test
    void test_save_maxNumberOfKeysInSegment_is_less_than_4() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class)//
                .withValueClass(String.class)//
                .withName("test_index")//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withThreadSafe(true)//
                .withLogEnabled(true)//
                .withMaxNumberOfKeysInCache(3)//
                .withMaxNumberOfKeysInSegment(3)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Max number of keys in segment must be at least 4.",
                ex.getMessage());
    }

    @Test
    void test_save_maxNumberOfSegmentsInCache_is_null() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class) //
                .withValueClass(String.class)//
                .withName("test_index")//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withThreadSafe(true)//
                .withLogEnabled(true)//
                .withMaxNumberOfKeysInCache(3)//
                .withMaxNumberOfKeysInSegment(4)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));
        assertEquals("Property ‘MaxNumberOfSegmentsInCache’ must not be null.",
                ex.getMessage());
    }

    @Test
    void test_save_maxNumberOfSegmentsInCache_is_less_than_3()
            throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class) //
                .withValueClass(String.class)//
                .withName("test_index")//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withThreadSafe(true)//
                .withLogEnabled(true)//
                .withMaxNumberOfKeysInCache(3)//
                .withMaxNumberOfKeysInSegment(4)//
                .withMaxNumberOfSegmentsInCache(1)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));
        assertEquals("Max number of segments in " + "cache must be at least 3.",
                ex.getMessage());
    }

    @Test
    void test_save_maxNumberOfKeysInSegmentCacheDuringFlushing_is_null()
            throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class) //
                .withValueClass(String.class)//
                .withName("test_index")//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withThreadSafe(true)//
                .withLogEnabled(true)//
                .withMaxNumberOfKeysInCache(3)//
                .withMaxNumberOfKeysInSegment(4)//
                .withMaxNumberOfSegmentsInCache(3)//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));
        assertEquals("Property ‘MaxNumberOfKeysInSegmentCacheDuringFlushing’"
                + " must not be null.", ex.getMessage());
    }

    @Test
    void test_save_maxNumberOfKeysInSegmentCacheDuringFlushing_is_less_than_3()
            throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class) //
                .withValueClass(String.class)//
                .withName("test_index")//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withThreadSafe(true)//
                .withLogEnabled(true)//
                .withMaxNumberOfKeysInCache(3)//
                .withMaxNumberOfKeysInSegment(4)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(2L) //
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
                .withName("test_index")//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withThreadSafe(true)//
                .withLogEnabled(true)//
                .withMaxNumberOfKeysInCache(3)//
                .withMaxNumberOfKeysInSegment(4)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withMaxNumberOfKeysInSegmentCache(11L)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(5L) //
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));
        assertEquals("Max number of keys in segment cache during "
                + "flushing must be greater than max number of "
                + "keys in segment cache.", ex.getMessage());
    }

    @Test
    void test_save_disk_reading_cache_size_in_not_1024() throws Exception {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class)//
                .withValueClass(String.class)//
                .withName("test_index")//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withThreadSafe(true)//
                .withLogEnabled(true)//
                .withMaxNumberOfKeysInSegmentCache(11L)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(22L) //
                .withMaxNumberOfKeysInSegmentIndexPage(33)//
                .withMaxNumberOfKeysInSegment(44)//
                .withMaxNumberOfSegmentsInCache(66)//
                .withMaxNumberOfKeysInCache(1000)
                .withDiskIoBufferSizeInBytes(1024)//
                .withBloomFilterIndexSizeInBytes(77)//
                .withBloomFilterNumberOfHashFunctions(88)//
                .withDiskIoBufferSizeInBytes(1000)//
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
    void test_save() throws Exception {
        manager.save(CONFIG);

        verify(storage, Mockito.times(1)).save(CONFIG);
    }

    @Test
    void test_mergeWithStored_used_stored_values() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withKeyClass(Long.class) //
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_LONG)//
                .withValueTypeDescriptor(TD_STRING)//
                .withName("test_index")//
                .withMaxNumberOfKeysInSegmentCache(11L)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(22L) //
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
        assertEquals(Long.class, ret.getKeyClass());
        assertEquals(String.class, ret.getValueClass());
        assertEquals(TD_LONG, ret.getKeyTypeDescriptor());
        assertEquals(TD_STRING, ret.getValueTypeDescriptor());
        assertEquals("test_index", ret.getIndexName());
        assertEquals(11L, ret.getMaxNumberOfKeysInSegmentCache());
        assertEquals(22L, ret.getMaxNumberOfKeysInSegmentCacheDuringFlushing());
        assertEquals(33, ret.getMaxNumberOfKeysInSegmentIndexPage());
        assertEquals(44, ret.getMaxNumberOfKeysInSegment());
        assertEquals(66, ret.getMaxNumberOfSegmentsInCache());
        assertEquals(1024, ret.getDiskIoBufferSize());
        assertEquals(77, ret.getBloomFilterIndexSizeInBytes());
        assertEquals(88, ret.getBloomFilterNumberOfHashFunctions());
        assertFalse(ret.isLogEnabled());
        assertFalse(ret.isThreadSafe());
    }

    @Test
    void test_mergeWithStored_indexName() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withName("pandemonium")//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final IndexConfiguration<Long, String> ret = manager
                .mergeWithStored(config);
        verify(storage, Mockito.times(1)).save(any());
        assertNotNull(ret);

        assertEquals("pandemonium", ret.getIndexName());
    }

    @Test
    void test_mergeWithStored_maxNumberOfKeysInSegmentCache() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withMaxNumberOfKeysInSegmentCache(3627L)//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final IndexConfiguration<Long, String> ret = manager
                .mergeWithStored(config);
        verify(storage, Mockito.times(1)).save(any());
        assertNotNull(ret);

        assertEquals(3627, ret.getMaxNumberOfKeysInSegmentCache());
    }

    @Test
    void test_mergeWithStored_diskIoBufferSize() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withDiskIoBufferSizeInBytes(1024 * 77)//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final IndexConfiguration<Long, String> ret = manager
                .mergeWithStored(config);
        verify(storage, Mockito.times(1)).save(any());
        assertNotNull(ret);

        assertEquals(1024 * 77, ret.getDiskIoBufferSize());
    }

    @Test
    void test_mergeWithStored_isLogEnabled() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withLogEnabled(true)//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final IndexConfiguration<Long, String> ret = manager
                .mergeWithStored(config);
        verify(storage, Mockito.times(1)).save(any());
        assertNotNull(ret);

        assertEquals(true, ret.isLogEnabled());
    }

    @Test
    void test_mergeWithStored_isThreadSafe() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withThreadSafe(true)//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final IndexConfiguration<Long, String> ret = manager
                .mergeWithStored(config);
        verify(storage, Mockito.times(1)).save(any());
        assertNotNull(ret);

        assertEquals(true, ret.isThreadSafe());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void test_mergeWithStored_keyClass() {
        final IndexConfiguration cfg = IndexConfiguration.builder()//
                .withKeyClass((Class) Double.class) //
                .build();
        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(cfg));

        assertEquals(
                "Key class is already set to 'java.lang.Long' and "
                        + "can't be changed to 'java.lang.Double'",
                e.getMessage());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void test_mergeWithStored_valueClass() {
        final IndexConfiguration cfg = IndexConfiguration.builder()//
                .withValueClass((Class) Double.class) //
                .build();
        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(cfg));

        assertEquals(
                "Value class is already set to 'java.lang.String' and "
                        + "can't be changed to 'java.lang.Double'",
                e.getMessage());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void test_mergeWithStored_keyTypeDescriptor() {
        final IndexConfiguration cfg = IndexConfiguration.builder()//
                .withKeyTypeDescriptor("kachana") //
                .build();
        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(cfg));

        assertEquals("Key type descriptor is already set to "
                + "'TypeDescriptorLong' and can't be changed to 'kachana'",
                e.getMessage());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void test_mergeWithStored_valueTypeDescriptor() {
        final IndexConfiguration cfg = IndexConfiguration.builder()//
                .withValueTypeDescriptor("kachna") //
                .build();
        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(cfg));

        assertEquals("Value type descriptor is already set to "
                + "'TypeDescriptorString' and can't be changed to 'kachna'",
                e.getMessage());
    }

    @Test
    void test_mergeWithStored_maxNumberOfKeysInSegment() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withMaxNumberOfKeysInSegment(9864)//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(config));

        assertEquals(
                "Value of MaxNumberOfKeysInSegment is already "
                        + "set to '44' and can't be changed to '9864'",
                e.getMessage());
    }

    @Test
    void test_mergeWithStored_bloomFilterIndexSizeInBytes() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withBloomFilterIndexSizeInBytes(4620)//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(config));

        assertEquals(
                "Value of BloomFilterIndexSizeInBytes is already "
                        + "set to '77' and can't be changed to '4620'",
                e.getMessage());
    }

    @Test
    void test_mergeWithStored_bloomFilterNumberOfHashFunctions() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withBloomFilterNumberOfHashFunctions(4620)//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(config));

        assertEquals(
                "Value of BloomFilterNumberOfHashFunctions is already "
                        + "set to '88' and can't be changed to '4620'",
                e.getMessage());
    }

    @Test
    void test_mergeWithStored_bloomFilterProbabilityOfFalsePositive() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withBloomFilterProbabilityOfFalsePositive(0.5)//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(config));

        assertEquals(
                "Value of BloomFilterProbabilityOfFalsePositive is already "
                        + "set to 'null' and can't be changed to '0.5'",
                e.getMessage());
    }

    @Test
    void test_mergeWithStored_maxNumberOfKeysInSegmentIndexPage() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .withMaxNumberOfKeysInSegmentIndexPage(4620)//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(config));

        assertEquals(
                "Value of MaxNumberOfKeysInSegmentIndexPage is already "
                        + "set to '33' and can't be changed to '4620'",
                e.getMessage());
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
                .withMaxNumberOfKeysInSegmentCache(11L)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(22L) //
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
