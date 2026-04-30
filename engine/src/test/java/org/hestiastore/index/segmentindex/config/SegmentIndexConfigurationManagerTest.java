package org.hestiastore.index.segmentindex.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.chunkstore.ChunkFilterRegistration;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.chunkstore.ChunkFilterSpecs;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexConfigurationBuilder;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexConfigurationManagerTest {

    private static final String TD_STRING = TypeDescriptorShortString.class
            .getSimpleName();
    private static final String TD_LONG = TypeDescriptorLong.class
            .getSimpleName();
    private static final IndexConfiguration<Long, String> CONFIG = IndexConfiguration
            .<Long, String>builder()//
            .identity(identity -> identity.keyClass(Long.class)) //
            .identity(identity -> identity.valueClass(String.class))//
            .identity(identity -> identity.keyTypeDescriptor(TD_LONG))//
            .identity(identity -> identity.valueTypeDescriptor(TD_STRING))//
            .identity(identity -> identity.name("test_index"))//
            .logging(logging -> logging.contextEnabled(false))//
            .segment(segment -> segment.cacheKeyLimit(11))//
            .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))//
            .segment(segment -> segment.chunkKeyLimit(33))//
            .segment(segment -> segment.deltaCacheFileLimit(7))//
            .segment(segment -> segment.maxKeys(44))//
            .segment(segment -> segment.cachedSegmentLimit(66))//
            .io(io -> io.diskBufferSizeBytes(1024))//
            .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(77))//
            .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(88))//
            .filters(filters -> filters.encodingFilterClasses(
                    List.of(ChunkFilterCrc32Writing.class,
                            ChunkFilterMagicNumberWriting.class)))//
            .filters(filters -> filters.decodingFilterClasses(List.of(
                    ChunkFilterMagicNumberValidation.class,
                    ChunkFilterCrc32Validation.class)))//
            .build();

    @Mock
    private IndexConfigurationStorage<Long, String> storage;

    private IndexConfigurationManager<Long, String> manager;

    @Test
    void test_save_key_class_is_null() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .identity(identity -> identity.valueClass(String.class))//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Key class wasn't specified", ex.getMessage());
    }

    @Test
    void test_save_value_class_is_null() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .identity(identity -> identity.keyClass(Long.class))//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Value class wasn't specified", ex.getMessage());
    }

    @Test
    void test_save_index_name_is_null() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .identity(identity -> identity.keyClass(Long.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Property 'indexName' must not be null.",
                ex.getMessage());
    }

    @Test
    void test_save_index_name_is_blank() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .identity(identity -> identity.keyClass(Long.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.name("   "))//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Property 'indexName' must not be blank.",
                ex.getMessage());
    }

    @Test
    void test_save_key_type_descriptor_is_null() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .identity(identity -> identity.keyClass(Long.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.valueTypeDescriptor(TD_STRING))//
                .identity(identity -> identity.name("test_index"))//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Property 'keyTypeDescriptor' must not be null.",
                ex.getMessage());
    }

    @Test
    void test_save_key_type_descriptor_is_empty() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .identity(identity -> identity.keyClass(Long.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(""))//
                .identity(identity -> identity.valueTypeDescriptor(TD_STRING))//
                .identity(identity -> identity.name("test_index"))//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Property 'keyTypeDescriptor' must not be blank.",
                ex.getMessage());
    }

    @Test
    void test_save_value_type_descriptor_is_empty() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .identity(identity -> identity.keyClass(Long.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(TD_LONG))//
                .identity(identity -> identity.valueTypeDescriptor(""))//
                .identity(identity -> identity.name("test_index"))//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Property 'valueTypeDescriptor' must not be blank.",
                ex.getMessage());
    }

    @Test
    void test_save_log_enabled_missing() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .identity(identity -> identity.keyClass(Long.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.name("test_index"))//
                .identity(identity -> identity.keyTypeDescriptor(TD_LONG))//
                .identity(identity -> identity.valueTypeDescriptor(TD_STRING))//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Property 'isContextLoggingEnabled' must not be null.",
                ex.getMessage());
    }

    @Test
    void test_save_maxNumberOfKeysInSegment_is_null() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .identity(identity -> identity.keyClass(Long.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.name("test_index"))//
                .identity(identity -> identity.keyTypeDescriptor(TD_LONG))//
                .identity(identity -> identity.valueTypeDescriptor(TD_STRING))//
                .logging(logging -> logging.contextEnabled(true))//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Property 'MaxNumberOfKeysInSegment' must not be null.",
                ex.getMessage());
    }

    @Test
    void test_save_maxNumberOfKeysInSegment_is_less_than_4() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .identity(identity -> identity.keyClass(Long.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.name("test_index"))//
                .identity(identity -> identity.keyTypeDescriptor(TD_LONG))//
                .identity(identity -> identity.valueTypeDescriptor(TD_STRING))//
                .logging(logging -> logging.contextEnabled(true))//
                .segment(segment -> segment.maxKeys(3))//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Max number of keys in segment must be at least 4.",
                ex.getMessage());
    }

    @Test
    void test_save_maxNumberOfSegmentsInCache_is_null() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .identity(identity -> identity.keyClass(Long.class)) //
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.name("test_index"))//
                .identity(identity -> identity.keyTypeDescriptor(TD_LONG))//
                .identity(identity -> identity.valueTypeDescriptor(TD_STRING))//
                .logging(logging -> logging.contextEnabled(true))//
                .segment(segment -> segment.maxKeys(4))//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));
        assertEquals("Property 'MaxNumberOfSegmentsInCache' must not be null.",
                ex.getMessage());
    }

    @Test
    void test_save_maxNumberOfSegmentsInCache_is_less_than_3() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .identity(identity -> identity.keyClass(Long.class)) //
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.name("test_index"))//
                .identity(identity -> identity.keyTypeDescriptor(TD_LONG))//
                .identity(identity -> identity.valueTypeDescriptor(TD_STRING))//
                .logging(logging -> logging.contextEnabled(true))//
                .segment(segment -> segment.maxKeys(4))//
                .segment(segment -> segment.cachedSegmentLimit(1))//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));
        assertEquals("Max number of segments in " + "cache must be at least 3.",
                ex.getMessage());
    }

    @Test
    void test_save_disk_reading_cache_size_in_not_1024() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .identity(identity -> identity.keyClass(Long.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.name("test_index"))//
                .identity(identity -> identity.keyTypeDescriptor(TD_LONG))//
                .identity(identity -> identity.valueTypeDescriptor(TD_STRING))//
                .logging(logging -> logging.contextEnabled(true))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))//
                .segment(segment -> segment.chunkKeyLimit(33))//
                .segment(segment -> segment.deltaCacheFileLimit(7))//
                .segment(segment -> segment.maxKeys(44))//
                .segment(segment -> segment.cachedSegmentLimit(66))//
                .io(io -> io.diskBufferSizeBytes(1024))//
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(77))//
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(88))//
                .io(io -> io.diskBufferSizeBytes(1000))//
                .identity(identity -> identity.name("test_index"))//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals(
                "Parameter 'diskIoBufferSize' with value '1000' "
                        + "can't be divided by 1024 without reminder",
                ex.getMessage());
    }

    @Test
    void test_save_disk_reading_cache_size_in_0() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .identity(identity -> identity.keyClass(Long.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.name("test_index"))//
                .identity(identity -> identity.keyTypeDescriptor(TD_LONG))//
                .identity(identity -> identity.valueTypeDescriptor(TD_STRING))//
                .logging(logging -> logging.contextEnabled(true))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))//
                .segment(segment -> segment.chunkKeyLimit(33))//
                .segment(segment -> segment.deltaCacheFileLimit(7))//
                .segment(segment -> segment.maxKeys(44))//
                .segment(segment -> segment.cachedSegmentLimit(66))//
                .io(io -> io.diskBufferSizeBytes(0))//
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(77))//
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(88))//
                .identity(identity -> identity.name("test_index"))//
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals(
                "Parameter 'diskIoBufferSize' with value '0' "
                        + "can't be smaller or equal to zero.",
                ex.getMessage());
    }

    @Test
    void test_save_encoding_filters_empty() {
        final IndexConfiguration<Long, String> config = baseBuilder()
                .filters(filters -> filters
                        .encodingFilters(List.<ChunkFilter>of()))
                .filters(filters -> filters.decodingFilterClasses(
                        List.of(ChunkFilterMagicNumberValidation.class)))
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Encoding chunk filters must not be empty.",
                ex.getMessage());
    }

    @Test
    void test_save_decoding_filters_empty() {
        final IndexConfiguration<Long, String> config = baseBuilder()
                .filters(filters -> filters.encodingFilterClasses(
                        List.of(ChunkFilterCrc32Writing.class)))
                .filters(filters -> filters
                        .decodingFilters(List.<ChunkFilter>of()))
                .build();

        final Exception ex = assertThrows(IllegalArgumentException.class,
                () -> manager.save(config));

        assertEquals("Decoding chunk filters must not be empty.",
                ex.getMessage());
    }

    @Test
    void test_applyDefaults_adds_default_filters() {
        final IndexConfiguration<Long, String> config = baseBuilder().build();

        final IndexConfiguration<Long, String> withDefaults = manager
                .applyDefaults(config);

        assertEquals(2, withDefaults.resolveRuntimeConfiguration()
                .getEncodingChunkFilters().size());
        assertEquals(ChunkFilterCrc32Writing.class,
                withDefaults.resolveRuntimeConfiguration()
                        .getEncodingChunkFilters().get(0).getClass());
        assertEquals(ChunkFilterMagicNumberWriting.class,
                withDefaults.resolveRuntimeConfiguration()
                        .getEncodingChunkFilters().get(1).getClass());

        assertEquals(2, withDefaults.resolveRuntimeConfiguration()
                .getDecodingChunkFilters().size());
        assertEquals(ChunkFilterMagicNumberValidation.class,
                withDefaults.resolveRuntimeConfiguration()
                        .getDecodingChunkFilters().get(0).getClass());
        assertEquals(ChunkFilterCrc32Validation.class,
                withDefaults.resolveRuntimeConfiguration()
                        .getDecodingChunkFilters().get(1).getClass());
        assertEquals(
                IndexConfigurationContract.DEFAULT_REGISTRY_LIFECYCLE_THREADS,
                withDefaults.maintenance().registryLifecycleThreads(),
                "Registry lifecycle threads should be defaulted");
    }

    @Test
    void test_applyDefaults_fills_missing_type_descriptors_from_registry() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .identity(identity -> identity.keyClass(Long.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.name("defaults-fill-type-descriptors"))//
                .build();

        final IndexConfiguration<Long, String> withDefaults = manager
                .applyDefaults(config);

        assertEquals(TypeDescriptorLong.class.getName(),
                withDefaults.identity().keyTypeDescriptor());
        assertEquals(TypeDescriptorShortString.class.getName(),
                withDefaults.identity().valueTypeDescriptor());
    }

    @Test
    void test_save() {
        manager.save(CONFIG);

        verify(storage, Mockito.times(1)).save(CONFIG);
    }

    @Test
    void test_mergeWithStored_used_stored_values() {
        when(storage.load()).thenReturn(CONFIG);
        final IndexConfiguration<Long, String> ret = manager
                .mergeWithStored(CONFIG);
        // verify that unchanged object is not saved
        verify(storage, Mockito.times(0)).save(any());

        assertNotNull(ret);
        assertEquals(Long.class, ret.identity().keyClass());
        assertEquals(String.class, ret.identity().valueClass());
        assertEquals(TD_LONG, ret.identity().keyTypeDescriptor());
        assertEquals(TD_STRING, ret.identity().valueTypeDescriptor());
        assertEquals("test_index", ret.identity().name());
        assertEquals(11, ret.segment().cacheKeyLimit());
        assertEquals(33, ret.segment().chunkKeyLimit());
        assertEquals(44, ret.segment().maxKeys());
        assertEquals(66, ret.segment().cachedSegmentLimit());
        assertEquals(1024, ret.io().diskBufferSizeBytes());
        assertEquals(77, ret.bloomFilter().indexSizeBytes());
        assertEquals(88, ret.bloomFilter().hashFunctions());
        assertFalse(ret.logging().contextEnabled());
    }

    @Test
    void test_mergeWithStored_indexName() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .identity(identity -> identity.name("pandemonium"))//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final IndexConfiguration<Long, String> ret = manager
                .mergeWithStored(config);
        verify(storage, Mockito.times(1)).save(any());
        assertNotNull(ret);

        assertEquals("pandemonium", ret.identity().name());
    }

    @Test
    void test_mergeWithStored_maxNumberOfKeysInSegmentCache() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .segment(segment -> segment.cacheKeyLimit(8))//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final IndexConfiguration<Long, String> ret = manager
                .mergeWithStored(config);
        verify(storage, Mockito.times(1)).save(any());
        assertNotNull(ret);

        assertEquals(8, ret.segment().cacheKeyLimit());
    }

    @Test
    void test_mergeWithStored_diskIoBufferSize() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .io(io -> io.diskBufferSizeBytes(1024 * 77))//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final IndexConfiguration<Long, String> ret = manager
                .mergeWithStored(config);
        verify(storage, Mockito.times(1)).save(any());
        assertNotNull(ret);

        assertEquals(1024 * 77, ret.io().diskBufferSizeBytes());
    }

    @Test
    void test_mergeWithStored_loggingContextEnabled() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .logging(logging -> logging.contextEnabled(true))//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final IndexConfiguration<Long, String> ret = manager
                .mergeWithStored(config);
        verify(storage, Mockito.times(1)).save(any());
        assertNotNull(ret);

        assertTrue(ret.logging().contextEnabled());
    }

    @Test
    void test_mergeWithStored_numberOfRegistryLifecycleThreads_defaultsToStoredValue() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final IndexConfiguration<Long, String> ret = manager
                .mergeWithStored(config);
        verify(storage, Mockito.times(1)).save(any());
        assertNotNull(ret);

        assertEquals(IndexConfigurationContract.DEFAULT_REGISTRY_LIFECYCLE_THREADS,
                ret.maintenance().registryLifecycleThreads());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void test_mergeWithStored_keyClass() {
        final IndexConfiguration cfg = IndexConfiguration.builder()//
                .identity(identity -> identity.keyClass((Class) Double.class)) //
                .build();
        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(cfg));

        assertEquals(
                "Value of 'KeyClass' is already set to 'java.lang.Long' "
                        + "and can't be changed to 'java.lang.Double'",
                e.getMessage());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void test_mergeWithStored_valueClass() {
        final IndexConfiguration cfg = IndexConfiguration.builder()//
                .identity(identity -> identity.valueClass((Class) Double.class)) //
                .build();
        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(cfg));

        assertEquals(
                "Value of 'ValueClass' is already set to 'java.lang.String' "
                        + "and can't be changed to 'java.lang.Double'",
                e.getMessage());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void test_mergeWithStored_keyTypeDescriptor() {
        final IndexConfiguration cfg = IndexConfiguration.builder()//
                .identity(identity -> identity.keyTypeDescriptor("kachana")) //
                .build();
        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(cfg));

        assertEquals("Value of 'KeyTypeDescriptor' is already set to "
                + "'TypeDescriptorLong' and can't be changed to 'kachana'",
                e.getMessage());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void test_mergeWithStored_valueTypeDescriptor() {
        final IndexConfiguration cfg = IndexConfiguration.builder()//
                .identity(identity -> identity.valueTypeDescriptor("kachna")) //
                .build();
        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(cfg));

        assertEquals("Value of 'ValueTypeDescriptor' is already set to "
                + "'TypeDescriptorShortString' and can't be changed to 'kachna'",
                e.getMessage());
    }

    @Test
    void test_mergeWithStored_maxNumberOfKeysInSegment() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .segment(segment -> segment.maxKeys(9864))//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(config));

        assertEquals(
                "Value of 'MaxNumberOfKeysInSegment' is already "
                        + "set to '44' and can't be changed to '9864'",
                e.getMessage());
    }

    @Test
    void test_mergeWithStored_bloomFilterIndexSizeInBytes() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(4620))//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(config));

        assertEquals(
                "Value of 'BloomFilterIndexSizeInBytes' is already "
                        + "set to '77' and can't be changed to '4620'",
                e.getMessage());
    }

    @Test
    void test_mergeWithStored_bloomFilterNumberOfHashFunctions() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(4620))//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(config));

        assertEquals(
                "Value of 'BloomFilterNumberOfHashFunctions' is already "
                        + "set to '88' and can't be changed to '4620'",
                e.getMessage());
    }

    @Test
    void test_mergeWithStored_bloomFilterProbabilityOfFalsePositive() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.5))//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(config));

        assertEquals(
                "Value of 'BloomFilterProbabilityOfFalsePositive' is already "
                        + "set to 'null' and can't be changed to '0.5'",
                e.getMessage());
    }

    @Test
    void test_mergeWithStored_maxNumberOfKeysInSegmentChunk() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .segment(segment -> segment.chunkKeyLimit(4620))//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(config));

        assertEquals(
                "Value of 'MaxNumberOfKeysInSegmentChunk' is already "
                        + "set to '33' and can't be changed to '4620'",
                e.getMessage());
    }

    @Test
    void test_mergeWithStored_encodingChunkFilters() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .filters(filters -> filters.encodingFilters(
                        List.of(new ChunkFilterCrc32Writing())))//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(config));

        assertEquals(
                "Value of 'EncodingChunkFilters' is already set to '"
                        + CONFIG.filters().encodingChunkFilterSpecs()
                        + "' and can't be changed to '"
                        + config.filters().encodingChunkFilterSpecs() + "'",
                e.getMessage());
    }

    @Test
    void test_mergeWithStored_decodingChunkFilters() {
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                .filters(filters -> filters.decodingFilters(
                        List.of(new ChunkFilterMagicNumberValidation())))//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> manager.mergeWithStored(config));

        assertEquals(
                "Value of 'DecodingChunkFilters' is already set to '"
                        + CONFIG.filters().decodingChunkFilterSpecs()
                        + "' and can't be changed to '"
                        + config.filters().decodingChunkFilterSpecs() + "'",
                e.getMessage());
    }

    @Test
    void test_mergeWithStored_encodingChunkFilters_same_classes_different_instances() {
        assertTrue(true);
        // stored configuration has two encoding filters configured by class in
        // CONFIG
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()//
                // Use the exact same filter classes as in CONFIG, but create
                // new instances.
                .filters(filters -> filters.encodingFilters(
                        List.of(new ChunkFilterCrc32Writing(),
                                new ChunkFilterMagicNumberWriting())))//
                .build();

        when(storage.load()).thenReturn(CONFIG);
        manager.mergeWithStored(config);
    }

    @Test
    void test_mergeWithStored_legacyBuiltInJavaClassSpecsAreEquivalent() {
        final IndexConfiguration<Long, String> storedConfig = baseBuilder()
                .filters(filters -> filters.encodingFilterRegistrations(List.of(
                        ChunkFilterRegistration.of(ChunkFilterSpecs.javaClass(
                                ChunkFilterCrc32Writing.class),
                                ChunkFilterCrc32Writing::new),
                        ChunkFilterRegistration.of(ChunkFilterSpecs.javaClass(
                                ChunkFilterMagicNumberWriting.class),
                                ChunkFilterMagicNumberWriting::new))))
                .filters(filters -> filters.decodingFilterRegistrations(List.of(
                        ChunkFilterRegistration.of(ChunkFilterSpecs.javaClass(
                                ChunkFilterMagicNumberValidation.class),
                                ChunkFilterMagicNumberValidation::new),
                        ChunkFilterRegistration.of(ChunkFilterSpecs.javaClass(
                                ChunkFilterCrc32Validation.class),
                                ChunkFilterCrc32Validation::new))))
                .build();
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()
                .filters(filters -> filters.encodingFilterClasses(
                        List.of(ChunkFilterCrc32Writing.class,
                                ChunkFilterMagicNumberWriting.class)))
                .filters(filters -> filters.decodingFilterClasses(List.of(
                        ChunkFilterMagicNumberValidation.class,
                        ChunkFilterCrc32Validation.class)))
                .build();

        when(storage.load()).thenReturn(storedConfig);

        final IndexConfiguration<Long, String> merged = manager
                .mergeWithStored(config);

        assertNotNull(merged);
        assertEquals(storedConfig.filters().encodingChunkFilterSpecs(),
                merged.filters().encodingChunkFilterSpecs());
        assertEquals(storedConfig.filters().decodingChunkFilterSpecs(),
                merged.filters().decodingChunkFilterSpecs());
        Mockito.verify(storage, Mockito.never()).save(any());
    }

    @Test
    void test_mergeWithStored_chunkFilterSpecParameterOrderDoesNotMatter() {
        final ChunkFilterSpec storedSpec = ChunkFilterSpec.ofProvider("custom")
                .withParameter("keyRef", "orders-main")
                .withParameter("mode", "gcm");
        final ChunkFilterSpec requestSpec = ChunkFilterSpec.ofProvider("custom")
                .withParameter("mode", "gcm")
                .withParameter("keyRef", "orders-main");
        final IndexConfiguration<Long, String> storedConfig = baseBuilder()
                .filters(filters -> filters.encodingFilterRegistrations(List.of(
                        ChunkFilterRegistration.of(storedSpec,
                                ChunkFilterDoNothing::new))))
                .filters(filters -> filters.decodingFilterRegistrations(List.of(
                        ChunkFilterRegistration.of(storedSpec,
                                ChunkFilterDoNothing::new))))
                .build();
        final IndexConfiguration<Long, String> config = IndexConfiguration
                .<Long, String>builder()
                .filters(filters -> filters
                        .addEncodingFilter(requestSpec)
                        .addDecodingFilter(requestSpec))
                .build();

        when(storage.load()).thenReturn(storedConfig);

        final IndexConfiguration<Long, String> merged = manager
                .mergeWithStored(config);

        assertNotNull(merged);
        assertEquals(storedConfig.filters().encodingChunkFilterSpecs(),
                merged.filters().encodingChunkFilterSpecs());
        assertEquals(storedConfig.filters().decodingChunkFilterSpecs(),
                merged.filters().decodingChunkFilterSpecs());
        Mockito.verify(storage, Mockito.never()).save(any());
    }

    @BeforeEach
    void setup() {
        manager = new IndexConfigurationManager<>(storage);
    }

    @AfterEach
    void tearDown() {
        manager = null;
    }

    private IndexConfigurationBuilder<Long, String> baseBuilder() {
        return IndexConfiguration.<Long, String>builder()//
                .identity(identity -> identity.keyClass(Long.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(TD_LONG))//
                .identity(identity -> identity.valueTypeDescriptor(TD_STRING))//
                .identity(identity -> identity.name("base_index"))//
                .logging(logging -> logging.contextEnabled(true))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))//
                .segment(segment -> segment.chunkKeyLimit(33))//
                .segment(segment -> segment.maxKeys(44))//
                .segment(segment -> segment.cachedSegmentLimit(66))//
                .io(io -> io.diskBufferSizeBytes(1024))//
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(77))//
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(88))//
                .filters(filters -> filters.encodingFilterSpecs(
                        CONFIG.filters().encodingChunkFilterSpecs()))//
                .filters(filters -> filters.decodingFilterSpecs(
                        CONFIG.filters().decodingChunkFilterSpecs()));
    }

}
