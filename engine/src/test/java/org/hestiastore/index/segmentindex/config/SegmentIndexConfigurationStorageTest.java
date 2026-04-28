package org.hestiastore.index.segmentindex.config;

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
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SegmentIndexConfigurationStorageTest {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(SegmentIndexConfigurationStorageTest.class);

    private static final String TD_STRING = TypeDescriptorShortString.class
            .getName();
    private static final String TD_LONG = TypeDescriptorLong.class.getName();
    private Directory directory;

    private IndexConfigurationStorage<String, Long> storage;

    private static final int MAX_KEYS_IN_SEGMENT_CACHE = 5000;
    private static final int MAX_KEYS_IN_ACTIVE_PARTITION = 2500;
    private static final int MAX_KEYS_IN_SEGMENT_CHUNK = 256;
    private static final int MAX_DELTA_CACHE_FILES = 12;
    private static final int MAX_KEYS_SEGMENT = 20000;
    private static final int MAX_KEYS_BEFORE_SPLIT = 30000;
    private static final int MAX_SEGMENTS_CACHE = 8;
    private static final int NUMBER_OF_STABLE_SEGMENT_MAINTENANCE_THREADS = 5;
    private static final int NUMBER_OF_REGISTRY_LIFECYCLE_THREADS = 4;
    private static final String INDEX_NAME = "specialIndex01";
    private static final int BLOOM_FILTER_HASH = 3;
    private static final int BLOOM_FILTER_INDEX_BYTES = 2048;
    private static final double BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE = 0.71;
    private static final int DISK_IO_BUFFER = 4096;

    @Test
    void saveAndLoadRoundTrip() {
        final IndexConfiguration<String, Long> config = IndexConfiguration
                .<String, Long>builder()//
                .identity(identity -> identity.keyClass(String.class))//
                .identity(identity -> identity.valueClass(Long.class))//
                .identity(identity -> identity.keyTypeDescriptor(TD_STRING))//
                .identity(identity -> identity.valueTypeDescriptor(TD_LONG))//
                .segment(segment -> segment.cacheKeyLimit(MAX_KEYS_IN_SEGMENT_CACHE))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(
                        MAX_KEYS_IN_ACTIVE_PARTITION))//
                .segment(segment -> segment.chunkKeyLimit(256))//
                .segment(segment -> segment.deltaCacheFileLimit(MAX_DELTA_CACHE_FILES))//
                .segment(segment -> segment.maxKeys(20000))//
                .writePath(writePath -> writePath.segmentSplitKeyThreshold(
                        MAX_KEYS_BEFORE_SPLIT))//
                .segment(segment -> segment.cachedSegmentLimit(8))//
                .maintenance(maintenance -> maintenance.segmentThreads(
                        NUMBER_OF_STABLE_SEGMENT_MAINTENANCE_THREADS))//
                .maintenance(maintenance -> maintenance.registryLifecycleThreads(
                        NUMBER_OF_REGISTRY_LIFECYCLE_THREADS))//
                .identity(identity -> identity.name(INDEX_NAME))//
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(3))//
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(2048))//
                .bloomFilter(bloomFilter -> bloomFilter
                        .falsePositiveProbability(
                                BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE))//
                .io(io -> io.diskBufferSizeBytes(4096))//
                .logging(logging -> logging.contextEnabled(true))//
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(true))//
                .build();
        storage.save(config);
        logConfigurationFile();

        final IndexConfiguration<String, Long> ret = storage.load();
        assertEquals(String.class, ret.identity().keyClass());
        assertEquals(Long.class, ret.identity().valueClass());
        assertEquals(TD_STRING, ret.identity().keyTypeDescriptor());
        assertEquals(TD_LONG, ret.identity().valueTypeDescriptor());
        assertEquals((int) MAX_KEYS_IN_SEGMENT_CACHE,
                ret.segment().cacheKeyLimit());
        assertEquals((int) MAX_KEYS_IN_ACTIVE_PARTITION,
                ret.writePath().segmentWriteCacheKeyLimit());
        assertEquals(MAX_KEYS_IN_SEGMENT_CHUNK,
                ret.segment().chunkKeyLimit());
        assertEquals(MAX_DELTA_CACHE_FILES,
                ret.segment().deltaCacheFileLimit());
        assertEquals(MAX_KEYS_SEGMENT, ret.segment().maxKeys());
        assertEquals(MAX_KEYS_BEFORE_SPLIT,
                ret.writePath().segmentSplitKeyThreshold());
        assertEquals(MAX_SEGMENTS_CACHE, ret.segment().cachedSegmentLimit());
        assertEquals(NUMBER_OF_STABLE_SEGMENT_MAINTENANCE_THREADS,
                ret.maintenance().segmentThreads());
        assertEquals(NUMBER_OF_REGISTRY_LIFECYCLE_THREADS,
                ret.maintenance().registryLifecycleThreads());
        assertEquals(INDEX_NAME, ret.identity().name());
        assertEquals(BLOOM_FILTER_HASH,
                ret.bloomFilter().hashFunctions());
        assertEquals(BLOOM_FILTER_INDEX_BYTES,
                ret.bloomFilter().indexSizeBytes());
        assertEquals(BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE,
                ret.bloomFilter().falsePositiveProbability());
        assertEquals(DISK_IO_BUFFER, ret.io().diskBufferSizeBytes());
        assertTrue(ret.logging().contextEnabled());
        assertTrue(ret.maintenance().backgroundAutoEnabled());
    }

    @Test
    void saveAndLoadRoundTripWithChunkFilters() {
        final IndexConfiguration<String, Long> config = IndexConfiguration
                .<String, Long>builder()//
                .identity(identity -> identity.keyClass(String.class))//
                .identity(identity -> identity.valueClass(Long.class))//
                .identity(identity -> identity.keyTypeDescriptor(TD_STRING))//
                .identity(identity -> identity.valueTypeDescriptor(TD_LONG))//
                .identity(identity -> identity.name(INDEX_NAME))//
                .segment(segment -> segment.cacheKeyLimit(MAX_KEYS_IN_SEGMENT_CACHE))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(
                        MAX_KEYS_IN_ACTIVE_PARTITION))//
                .segment(segment -> segment.chunkKeyLimit(MAX_KEYS_IN_SEGMENT_CHUNK))//
                .segment(segment -> segment.deltaCacheFileLimit(MAX_DELTA_CACHE_FILES))//
                .segment(segment -> segment.maxKeys(MAX_KEYS_SEGMENT))//
                .writePath(writePath -> writePath.segmentSplitKeyThreshold(
                        MAX_KEYS_BEFORE_SPLIT))//
                .segment(segment -> segment.cachedSegmentLimit(MAX_SEGMENTS_CACHE))//
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(BLOOM_FILTER_HASH))//
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(BLOOM_FILTER_INDEX_BYTES))//
                .bloomFilter(bloomFilter -> bloomFilter
                        .falsePositiveProbability(
                                BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE))//
                .io(io -> io.diskBufferSizeBytes(DISK_IO_BUFFER))//
                .logging(logging -> logging.contextEnabled(true))//
                .filters(filters -> filters.encodingFilterClasses(List.of(
                        ChunkFilterCrc32Writing.class,
                        ChunkFilterMagicNumberWriting.class,
                        ChunkFilterSnappyCompress.class)))//
                .filters(filters -> filters.decodingFilterClasses(List.of(
                        ChunkFilterSnappyDecompress.class,
                        ChunkFilterMagicNumberValidation.class,
                        ChunkFilterCrc32Validation.class)))//
                .build();
        storage.save(config);
        logConfigurationFile();

        final IndexConfiguration<String, Long> loaded = storage.load();
        assertEquals(3, loaded.resolveRuntimeConfiguration().getEncodingChunkFilters().size());
        assertEquals(ChunkFilterCrc32Writing.class,
                loaded.resolveRuntimeConfiguration().getEncodingChunkFilters().get(0).getClass());
        assertEquals(ChunkFilterMagicNumberWriting.class,
                loaded.resolveRuntimeConfiguration().getEncodingChunkFilters().get(1).getClass());
        assertEquals(ChunkFilterSnappyCompress.class,
                loaded.resolveRuntimeConfiguration().getEncodingChunkFilters().get(2).getClass());

        assertEquals(3, loaded.resolveRuntimeConfiguration().getDecodingChunkFilters().size());
        assertEquals(ChunkFilterSnappyDecompress.class,
                loaded.resolveRuntimeConfiguration().getDecodingChunkFilters().get(0).getClass());
        assertEquals(ChunkFilterMagicNumberValidation.class,
                loaded.resolveRuntimeConfiguration().getDecodingChunkFilters().get(1).getClass());
        assertEquals(ChunkFilterCrc32Validation.class,
                loaded.resolveRuntimeConfiguration().getDecodingChunkFilters().get(2).getClass());
    }

    @Test
    void saveAndLoadRoundTripWithDefaultFalsePositiveProbability() {
        final IndexConfiguration<String, Long> config = IndexConfiguration
                .<String, Long>builder()//
                .identity(identity -> identity.keyClass(String.class))//
                .identity(identity -> identity.valueClass(Long.class))//
                .identity(identity -> identity.keyTypeDescriptor(TD_STRING))//
                .identity(identity -> identity.valueTypeDescriptor(TD_LONG))//
                .segment(segment -> segment.cacheKeyLimit(MAX_KEYS_IN_SEGMENT_CACHE))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(
                        MAX_KEYS_IN_ACTIVE_PARTITION))//
                .segment(segment -> segment.chunkKeyLimit(256))//
                .segment(segment -> segment.deltaCacheFileLimit(MAX_DELTA_CACHE_FILES))//
                .segment(segment -> segment.maxKeys(20000))//
                .writePath(writePath -> writePath.segmentSplitKeyThreshold(
                        MAX_KEYS_BEFORE_SPLIT))//
                .segment(segment -> segment.cachedSegmentLimit(8))//
                .identity(identity -> identity.name(INDEX_NAME))//
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(3))//
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(2048))//
                .io(io -> io.diskBufferSizeBytes(4096))//
                .logging(logging -> logging.contextEnabled(true))//
                .build();
        storage.save(config);
        logConfigurationFile();

        final IndexConfiguration<String, Long> ret = storage.load();
        assertEquals(String.class, ret.identity().keyClass());
        assertEquals(Long.class, ret.identity().valueClass());
        assertEquals(TD_STRING, ret.identity().keyTypeDescriptor());
        assertEquals(TD_LONG, ret.identity().valueTypeDescriptor());
        assertEquals((int) MAX_KEYS_IN_SEGMENT_CACHE,
                ret.segment().cacheKeyLimit());
        assertEquals((int) MAX_KEYS_IN_ACTIVE_PARTITION,
                ret.writePath().segmentWriteCacheKeyLimit());
        assertEquals(MAX_KEYS_IN_SEGMENT_CHUNK,
                ret.segment().chunkKeyLimit());
        assertEquals(MAX_DELTA_CACHE_FILES,
                ret.segment().deltaCacheFileLimit());
        assertEquals(MAX_KEYS_SEGMENT, ret.segment().maxKeys());
        assertEquals(MAX_KEYS_BEFORE_SPLIT,
                ret.writePath().segmentSplitKeyThreshold());
        assertEquals(MAX_SEGMENTS_CACHE, ret.segment().cachedSegmentLimit());
        assertEquals(INDEX_NAME, ret.identity().name());
        assertEquals(BLOOM_FILTER_HASH,
                ret.bloomFilter().hashFunctions());
        assertEquals(BLOOM_FILTER_INDEX_BYTES,
                ret.bloomFilter().indexSizeBytes());
        /**
         * verify that bloom fileter probability of false positive is set to
         * default
         */
        assertEquals(BloomFilterBuilder.DEFAULT_PROBABILITY_OF_FALSE_POSITIVE,
                ret.bloomFilter().falsePositiveProbability());
        assertEquals(DISK_IO_BUFFER, ret.io().diskBufferSizeBytes());
        assertTrue(ret.logging().contextEnabled());
    }

    @Test
    void loadMissingConfigurationFileFails() {
        final Exception e = assertThrows(IndexException.class,
                () -> storage.load());

        assertTrue(e.getMessage().startsWith(
                "File manifest.txt does not exist in directory"),
                e.getMessage());
    }

    @BeforeEach
    void setup() {
        directory = new MemDirectory();
        storage = new IndexConfigurationStorage<>(
                directory);
    }

    @AfterEach
    void tearDown() {
        storage = null;
        directory = null;
    }

    private void logConfigurationFile() {
        if (directory instanceof MemDirectory) {
            final MemDirectory memDirectory = (MemDirectory) directory;
            final String fileName = "manifest.txt";
            if (memDirectory.isFileExists(fileName)) {
                final String content = new String(
                        memDirectory.getFileSequence(fileName)
                                .toByteArrayCopy(),
                        StandardCharsets.UTF_8);
                LOGGER.info("{}:{}{}", fileName, System.lineSeparator(),
                        content);
            } else {
                LOGGER.info("{} not present in directory", fileName);
            }
        }
    }

}
