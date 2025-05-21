package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.coroptis.index.IndexException;
import com.coroptis.index.bloomfilter.BloomFilterBuilder;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorLong;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class IndexConfiguratonStorageTest {

    private final static TypeDescriptor<String> TD_STRING = new TypeDescriptorString();
    private final static TypeDescriptor<Long> TD_LONG = new TypeDescriptorLong();
    private Directory directory;

    private IndexConfiguratonStorage<String, Long> storage;

    private static final long MAX_KEYS_IN_SEGMENT_CACHE = 5000L;
    private static final int MAX_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING = 5777;
    private static final int MAX_INDEX_PAGE = 256;
    private static final int MAX_KEYS_CACHE = 10000;
    private static final int MAX_KEYS_SEGMENT = 20000;
    private static final int MAX_SEGMENTS_CACHE = 8;
    private static final String INDX_NAME = "specialIndex01";
    private static final int BLOOM_FILTER_HASH = 3;
    private static final int BLOOM_FILTER_INDEX_BYTES = 2048;
    private static final double BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE = 0.71;
    private static final int DISK_IO_BUFFER = 4096;

    @Test
    void test_save_and_load() {
        final IndexConfiguration<String, Long> config = IndexConfiguration
                .<String, Long>builder()//
                .withKeyClass(String.class)//
                .withValueClass(Long.class)//
                .withKeyTypeDescriptor(TD_STRING)//
                .withValueTypeDescriptor(TD_LONG)//
                .withMaxNumberOfKeysInSegmentCache(MAX_KEYS_IN_SEGMENT_CACHE)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(
                        MAX_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING)//
                .withMaxNumberOfKeysInSegmentIndexPage(256)//
                .withMaxNumberOfKeysInCache(10000)//
                .withMaxNumberOfKeysInSegment(20000)//
                .withMaxNumberOfSegmentsInCache(8)//
                .withName(INDX_NAME)//
                .withCustomConf()//
                .withBloomFilterNumberOfHashFunctions(3)//
                .withBloomFilterIndexSizeInBytes(2048)//
                .withBloomFilterProbabilityOfFalsePositive(
                        BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE)//
                .withDiskIoBufferSizeInBytes(4096)//
                .withIsIndexSynchronized(true)//
                .withUseFullLog(true)//
                .build();
        storage.save(config);

        final IndexConfiguration<String, Long> ret = storage.load();
        assertEquals(String.class, ret.getKeyClass());
        assertEquals(Long.class, ret.getValueClass());
        assertEquals(TD_STRING, ret.getKeyTypeDescriptor());
        assertEquals(TD_LONG, ret.getValueTypeDescriptor());
        assertEquals(MAX_KEYS_IN_SEGMENT_CACHE,
                ret.getMaxNumberOfKeysInSegmentCache());
        assertEquals(MAX_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING,
                ret.getMaxNumberOfKeysInSegmentCacheDuringFlushing());
        assertEquals(MAX_INDEX_PAGE,
                ret.getMaxNumberOfKeysInSegmentIndexPage());
        assertEquals(MAX_KEYS_CACHE, ret.getMaxNumberOfKeysInCache());
        assertEquals(MAX_KEYS_SEGMENT, ret.getMaxNumberOfKeysInSegment());
        assertEquals(MAX_SEGMENTS_CACHE, ret.getMaxNumberOfSegmentsInCache());
        assertEquals(INDX_NAME, ret.getIndexName());
        assertEquals(BLOOM_FILTER_HASH,
                ret.getBloomFilterNumberOfHashFunctions());
        assertEquals(BLOOM_FILTER_INDEX_BYTES,
                ret.getBloomFilterIndexSizeInBytes());
        assertEquals(BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE,
                ret.getBloomFilterProbabilityOfFalsePositive());
        assertEquals(DISK_IO_BUFFER, ret.getDiskIoBufferSize());
        assertTrue(ret.isThreadSafe());
        assertTrue(ret.isLogEnabled());
    }

    @Test
    void test_save_and_load_empty_probability_of_false_positive() {
        final IndexConfiguration<String, Long> config = IndexConfiguration
                .<String, Long>builder()//
                .withKeyClass(String.class)//
                .withValueClass(Long.class)//
                .withKeyTypeDescriptor(TD_STRING)//
                .withValueTypeDescriptor(TD_LONG)//
                .withMaxNumberOfKeysInSegmentCache(MAX_KEYS_IN_SEGMENT_CACHE)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(
                        MAX_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING)//
                .withMaxNumberOfKeysInSegmentIndexPage(256)//
                .withMaxNumberOfKeysInCache(10000)//
                .withMaxNumberOfKeysInSegment(20000)//
                .withMaxNumberOfSegmentsInCache(8)//
                .withName(INDX_NAME)//
                .withCustomConf()//
                .withBloomFilterNumberOfHashFunctions(3)//
                .withBloomFilterIndexSizeInBytes(2048)//
                .withDiskIoBufferSizeInBytes(4096)//
                .withIsIndexSynchronized(true)//
                .withUseFullLog(true)//
                .build();
        storage.save(config);

        final IndexConfiguration<String, Long> ret = storage.load();
        assertEquals(String.class, ret.getKeyClass());
        assertEquals(Long.class, ret.getValueClass());
        assertEquals(TD_STRING, ret.getKeyTypeDescriptor());
        assertEquals(TD_LONG, ret.getValueTypeDescriptor());
        assertEquals(MAX_KEYS_IN_SEGMENT_CACHE,
                ret.getMaxNumberOfKeysInSegmentCache());
        assertEquals(MAX_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING,
                ret.getMaxNumberOfKeysInSegmentCacheDuringFlushing());
        assertEquals(MAX_INDEX_PAGE,
                ret.getMaxNumberOfKeysInSegmentIndexPage());
        assertEquals(MAX_KEYS_CACHE, ret.getMaxNumberOfKeysInCache());
        assertEquals(MAX_KEYS_SEGMENT, ret.getMaxNumberOfKeysInSegment());
        assertEquals(MAX_SEGMENTS_CACHE, ret.getMaxNumberOfSegmentsInCache());
        assertEquals(INDX_NAME, ret.getIndexName());
        assertEquals(BLOOM_FILTER_HASH,
                ret.getBloomFilterNumberOfHashFunctions());
        assertEquals(BLOOM_FILTER_INDEX_BYTES,
                ret.getBloomFilterIndexSizeInBytes());
        /**
         * verify that bloom fileter probability of false positive is set to
         * default
         */
        assertEquals(BloomFilterBuilder.DEFAULT_PROBABILITY_OF_FALSE_POSITIVE,
                ret.getBloomFilterProbabilityOfFalsePositive());
        assertEquals(DISK_IO_BUFFER, ret.getDiskIoBufferSize());
        assertTrue(ret.isThreadSafe());
        assertTrue(ret.isLogEnabled());
    }

    @Test
    void test_load_not_existing_file() {
        final Exception e = assertThrows(IndexException.class,
                () -> storage.load());

        assertEquals("File index-configuration.properties does not "
                + "exist in directory MemDirectory{}", e.getMessage());
    }

    @BeforeEach
    void setup() {
        directory = new MemDirectory();
        storage = new IndexConfiguratonStorage<>(directory);
    }

    @AfterEach
    void tearDown() {
        storage = null;
        directory = null;
    }

}
