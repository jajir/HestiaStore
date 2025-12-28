package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.bloomfilter.BloomFilterBuilder;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.chunkstore.ChunkFilterSnappyCompress;
import org.hestiastore.index.chunkstore.ChunkFilterSnappyDecompress;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SegmentIndexConfiguratonStorageTest {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(SegmentIndexConfiguratonStorageTest.class);

    private static final String TD_STRING = TypeDescriptorShortString.class
            .getName();
    private static final String TD_LONG = TypeDescriptorLong.class.getName();
    private Directory directory;

    private IndexConfiguratonStorage<String, Long> storage;

    private static final int MAX_KEYS_IN_SEGMENT_CACHE = 5000;
    private static final int MAX_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING = 5777;
    private static final int MAX_INDEX_PAGE = 256;
    private static final int MAX_KEYS_CACHE = 10000;
    private static final int MAX_KEYS_SEGMENT = 20000;
    private static final int MAX_SEGMENTS_CACHE = 8;
    private static final int NUMBER_OF_THREADS = 2;
    private static final int NUMBER_OF_IO_THREADS = 2;
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
                .withMaxNumberOfKeysInSegmentChunk(256)//
                .withMaxNumberOfKeysInCache(10000)//
                .withMaxNumberOfKeysInSegment(20000)//
                .withMaxNumberOfSegmentsInCache(8)//
                .withNumberOfCpuThreads(NUMBER_OF_THREADS)//
                .withNumberOfIoThreads(NUMBER_OF_IO_THREADS)//
                .withName(INDX_NAME)//
                .withBloomFilterNumberOfHashFunctions(3)//
                .withBloomFilterIndexSizeInBytes(2048)//
                .withBloomFilterProbabilityOfFalsePositive(
                        BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE)//
                .withDiskIoBufferSizeInBytes(4096)//
                .withContextLoggingEnabled(true)//
                .build();
        storage.save(config);
        logConfigurationFile();

        final IndexConfiguration<String, Long> ret = storage.load();
        assertEquals(String.class, ret.getKeyClass());
        assertEquals(Long.class, ret.getValueClass());
        assertEquals(TD_STRING, ret.getKeyTypeDescriptor());
        assertEquals(TD_LONG, ret.getValueTypeDescriptor());
        assertEquals((int) MAX_KEYS_IN_SEGMENT_CACHE,
                ret.getMaxNumberOfKeysInSegmentCache());
        assertEquals((int) MAX_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING,
                ret.getMaxNumberOfKeysInSegmentCacheDuringFlushing());
        assertEquals(MAX_INDEX_PAGE, ret.getMaxNumberOfKeysInSegmentChunk());
        assertEquals(MAX_KEYS_CACHE, ret.getMaxNumberOfKeysInCache());
        assertEquals(MAX_KEYS_SEGMENT, ret.getMaxNumberOfKeysInSegment());
        assertEquals(MAX_SEGMENTS_CACHE, ret.getMaxNumberOfSegmentsInCache());
        assertEquals(NUMBER_OF_THREADS, ret.getNumberOfThreads());
        assertEquals(NUMBER_OF_IO_THREADS, ret.getNumberOfIoThreads());
        assertEquals(INDX_NAME, ret.getIndexName());
        assertEquals(BLOOM_FILTER_HASH,
                ret.getBloomFilterNumberOfHashFunctions());
        assertEquals(BLOOM_FILTER_INDEX_BYTES,
                ret.getBloomFilterIndexSizeInBytes());
        assertEquals(BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE,
                ret.getBloomFilterProbabilityOfFalsePositive());
        assertEquals(DISK_IO_BUFFER, ret.getDiskIoBufferSize());
        assertTrue(ret.isContextLoggingEnabled());
    }

    @Test
    void test_save_and_load_with_filters() {
        final IndexConfiguration<String, Long> config = IndexConfiguration
                .<String, Long>builder()//
                .withKeyClass(String.class)//
                .withValueClass(Long.class)//
                .withKeyTypeDescriptor(TD_STRING)//
                .withValueTypeDescriptor(TD_LONG)//
                .withName(INDX_NAME)//
                .withMaxNumberOfKeysInSegmentCache(MAX_KEYS_IN_SEGMENT_CACHE)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(
                        MAX_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING)//
                .withMaxNumberOfKeysInSegmentChunk(MAX_INDEX_PAGE)//
                .withMaxNumberOfKeysInCache(MAX_KEYS_CACHE)//
                .withMaxNumberOfKeysInSegment(MAX_KEYS_SEGMENT)//
                .withMaxNumberOfSegmentsInCache(MAX_SEGMENTS_CACHE)//
                .withBloomFilterNumberOfHashFunctions(BLOOM_FILTER_HASH)//
                .withBloomFilterIndexSizeInBytes(BLOOM_FILTER_INDEX_BYTES)//
                .withBloomFilterProbabilityOfFalsePositive(
                        BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE)//
                .withDiskIoBufferSizeInBytes(DISK_IO_BUFFER)//
                .withContextLoggingEnabled(true)//
                .withEncodingFilterClasses(//
                        List.of(ChunkFilterCrc32Writing.class, //
                                ChunkFilterMagicNumberWriting.class, //
                                ChunkFilterSnappyCompress.class//
                        ))//
                .withDecodingFilterClasses(//
                        List.of(ChunkFilterSnappyDecompress.class, //
                                ChunkFilterMagicNumberValidation.class, //
                                ChunkFilterCrc32Validation.class//
                        ))//
                .build();
        storage.save(config);
        logConfigurationFile();

        final IndexConfiguration<String, Long> loaded = storage.load();
        assertEquals(3, loaded.getEncodingChunkFilters().size());
        assertEquals(ChunkFilterCrc32Writing.class,
                loaded.getEncodingChunkFilters().get(0).getClass());
        assertEquals(ChunkFilterMagicNumberWriting.class,
                loaded.getEncodingChunkFilters().get(1).getClass());
        assertEquals(ChunkFilterSnappyCompress.class,
                loaded.getEncodingChunkFilters().get(2).getClass());

        assertEquals(3, loaded.getDecodingChunkFilters().size());
        assertEquals(ChunkFilterSnappyDecompress.class,
                loaded.getDecodingChunkFilters().get(0).getClass());
        assertEquals(ChunkFilterMagicNumberValidation.class,
                loaded.getDecodingChunkFilters().get(1).getClass());
        assertEquals(ChunkFilterCrc32Validation.class,
                loaded.getDecodingChunkFilters().get(2).getClass());
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
                .withMaxNumberOfKeysInSegmentChunk(256)//
                .withMaxNumberOfKeysInCache(10000)//
                .withMaxNumberOfKeysInSegment(20000)//
                .withMaxNumberOfSegmentsInCache(8)//
                .withName(INDX_NAME)//
                .withBloomFilterNumberOfHashFunctions(3)//
                .withBloomFilterIndexSizeInBytes(2048)//
                .withDiskIoBufferSizeInBytes(4096)//
                .withContextLoggingEnabled(true)//
                .build();
        storage.save(config);
        logConfigurationFile();

        final IndexConfiguration<String, Long> ret = storage.load();
        assertEquals(String.class, ret.getKeyClass());
        assertEquals(Long.class, ret.getValueClass());
        assertEquals(TD_STRING, ret.getKeyTypeDescriptor());
        assertEquals(TD_LONG, ret.getValueTypeDescriptor());
        assertEquals((int) MAX_KEYS_IN_SEGMENT_CACHE,
                ret.getMaxNumberOfKeysInSegmentCache());
        assertEquals((int) MAX_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING,
                ret.getMaxNumberOfKeysInSegmentCacheDuringFlushing());
        assertEquals(MAX_INDEX_PAGE, ret.getMaxNumberOfKeysInSegmentChunk());
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
        assertTrue(ret.isContextLoggingEnabled());
    }

    @Test
    void test_load_not_existing_file() {
        final Exception e = assertThrows(IndexException.class,
                () -> storage.load());

        assertTrue(e.getMessage().startsWith(
                "File index-configuration.properties does not exist in directory"),
                e.getMessage());
    }

    @BeforeEach
    void setup() {
        directory = new MemDirectory();
        storage = new IndexConfiguratonStorage<>(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(directory));
    }

    @AfterEach
    void tearDown() {
        storage = null;
        directory = null;
    }

    private void logConfigurationFile() {
        if (directory instanceof MemDirectory) {
            final MemDirectory memDirectory = (MemDirectory) directory;
            final String fileName = "index-configuration.properties";
            if (memDirectory.isFileExists(fileName)) {
                final String content = new String(
                        memDirectory.getFileBytes(fileName).getData(),
                        StandardCharsets.UTF_8);
                LOGGER.info("{}:{}{}", fileName, System.lineSeparator(),
                        content);
            } else {
                LOGGER.info("{} not present in directory", fileName);
            }
        }
    }

}
