package org.hestiastore.index.segmentindex;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bloomfilter.BloomFilterBuilder;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.properties.PropertyStore;
import org.hestiastore.index.properties.PropertyStoreimpl;
import org.hestiastore.index.properties.PropertyTransaction;
import org.hestiastore.index.properties.PropertyView;
import org.hestiastore.index.properties.PropertyWriter;

public class IndexConfiguratonStorage<K, V> {

    private static final String PROP_KEY_CLASS = "keyClass";
    private static final String PROP_VALUE_CLASS = "valueClass";
    private static final String PROP_KEY_TYPE_DESCRIPTOR = "keyTypeDescriptor";
    private static final String PROP_VALUE_TYPE_DESCRIPTOR = "valueTypeDescriptor";
    private static final String PROP_INDEX_NAME = "indexName";
    private static final String PROP_CONTEXT_LOGGING_ENABLED = "contextLoggingEnabled";

    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE = "maxNumberOfKeysInSegmentCache";
    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE = "maxNumberOfKeysInSegmentWriteCache";
    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING = "maxNumberOfKeysInSegmentCacheDuringFlushing";
    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK = "maxNumberOfKeysInSegmentChunk";
    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_CACHE = "maxNumberOfKeysInCache";
    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT = "maxNumberOfKeysInSegment";
    private static final String PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE = "maxNumberOfSegmentsInCache";
    private static final String PROP_NUMBER_OF_THREADS = "numberOfThreads";
    private static final String PROP_NUMBER_OF_IO_THREADS = "numberOfIoThreads";
    private static final String PROP_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS = "bloomFilterNumberOfHashFunctions";
    private static final String PROP_BLOOM_FILTER_INDEX_SIZE_IN_BYTES = "bloomFilterIndexSizeInBytes";
    private static final String PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE = "bloomFilterProbabilityOfFalsePositive";
    private static final String PROP_DISK_IO_BUFFER_SIZE_IN_BYTES = "diskIoBufferSizeInBytes";
    private static final String PROP_ENCODING_CHUNK_FILTERS = "encodingChunkFilters";
    private static final String PROP_DECODING_CHUNK_FILTERS = "decodingChunkFilters";

    private static final String CONFIGURATION_FILENAME = "index-configuration.properties";

    private final AsyncDirectory directoryFacade;

    IndexConfiguratonStorage(final AsyncDirectory directoryFacade) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
    }

    IndexConfiguration<K, V> load() {
        final PropertyStore props = PropertyStoreimpl.fromAsyncDirectory(
                directoryFacade, CONFIGURATION_FILENAME, true);
        final PropertyView propsView = props.snapshot();
        final Class<K> keyClass = toClass(propsView.getString(PROP_KEY_CLASS));
        final Class<V> valueClass = toClass(
                propsView.getString(PROP_VALUE_CLASS));
        final long maxNumberOfKeysInSegmentCache = propsView
                .getLong(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE);
        final long defaultMaxNumberOfKeysInSegmentWriteCache = maxNumberOfKeysInSegmentCache > 0
                ? maxNumberOfKeysInSegmentCache / 2
                : IndexConfigurationContract.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE
                        / 2;
        final long maxNumberOfKeysInSegmentWriteCache = getOrDefaultLong(
                propsView, PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE,
                defaultMaxNumberOfKeysInSegmentWriteCache);
        final IndexConfigurationBuilder<K, V> builder = IndexConfiguration
                .<K, V>builder()//
                .withKeyClass(keyClass) //
                .withValueClass(valueClass)//
                .withName(propsView.getString(PROP_INDEX_NAME))//
                .withContextLoggingEnabled(
                        propsView.getBoolean(PROP_CONTEXT_LOGGING_ENABLED))//

                // SegmentIndex runtime properties
                .withMaxNumberOfKeysInCache(
                        propsView.getInt(PROP_MAX_NUMBER_OF_KEYS_IN_CACHE))//
                .withMaxNumberOfSegmentsInCache(
                        propsView.getInt(PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE))//
                .withMaxNumberOfKeysInSegment(
                        propsView.getInt(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT))//
                .withDiskIoBufferSizeInBytes(
                        propsView.getInt(PROP_DISK_IO_BUFFER_SIZE_IN_BYTES))//

                // Segment properties
                .withMaxNumberOfKeysInSegmentCache(
                        (int) maxNumberOfKeysInSegmentCache)//
                .withMaxNumberOfKeysInSegmentWriteCache(
                        (int) maxNumberOfKeysInSegmentWriteCache)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(
                        (int) propsView.getLong(
                                PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING))//
                .withMaxNumberOfKeysInSegmentChunk(
                        propsView.getInt(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK))//
                .withNumberOfCpuThreads(getOrDefault(propsView,
                        PROP_NUMBER_OF_THREADS,
                        IndexConfigurationContract.NUMBER_OF_THREADS))//
                .withNumberOfIoThreads(getOrDefault(propsView,
                        PROP_NUMBER_OF_IO_THREADS,
                        IndexConfigurationContract.NUMBER_OF_IO_THREADS))//

                // Segment bloom filter properties
                .withBloomFilterNumberOfHashFunctions(propsView
                        .getInt(PROP_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS))//
                .withBloomFilterIndexSizeInBytes(
                        propsView.getInt(PROP_BLOOM_FILTER_INDEX_SIZE_IN_BYTES))//
        ;

        if (propsView.getDouble(
                PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE) != 0) {
            builder.withBloomFilterProbabilityOfFalsePositive(propsView.getDouble(
                    PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE));
        }

        builder.withKeyTypeDescriptor(
                propsView.getString(PROP_KEY_TYPE_DESCRIPTOR));
        builder.withValueTypeDescriptor(
                propsView.getString(PROP_VALUE_TYPE_DESCRIPTOR));

        final String encodingFilters = propsView
                .getString(PROP_ENCODING_CHUNK_FILTERS);
        if (encodingFilters != null && !encodingFilters.isBlank()) {
            builder.withEncodingFilters(parseFilterList(encodingFilters));
        }

        final String decodingFilters = propsView
                .getString(PROP_DECODING_CHUNK_FILTERS);
        if (decodingFilters != null && !decodingFilters.isBlank()) {
            builder.withDecodingFilters(parseFilterList(decodingFilters));
        }

        return builder.build();
    }

    public void save(IndexConfiguration<K, V> indexConfiguration) {
        final PropertyStore props = PropertyStoreimpl.fromAsyncDirectory(
                directoryFacade, CONFIGURATION_FILENAME, false);
        final PropertyTransaction tx = props.beginTransaction();
        final PropertyWriter writer = tx.openPropertyWriter();
        writer.setString(PROP_KEY_CLASS,
                indexConfiguration.getKeyClass().getName());
        writer.setString(PROP_VALUE_CLASS,
                indexConfiguration.getValueClass().getName());
        writer.setString(PROP_KEY_TYPE_DESCRIPTOR,
                indexConfiguration.getKeyTypeDescriptor());
        writer.setString(PROP_VALUE_TYPE_DESCRIPTOR,
                indexConfiguration.getValueTypeDescriptor());
        writer.setString(PROP_INDEX_NAME, indexConfiguration.getIndexName());
        writer.setBoolean(PROP_CONTEXT_LOGGING_ENABLED,
                indexConfiguration.isContextLoggingEnabled());

        // SegmentIndex runtime properties
        writer.setLong(PROP_MAX_NUMBER_OF_KEYS_IN_CACHE,
                indexConfiguration.getMaxNumberOfKeysInCache());
        writer.setInt(PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                indexConfiguration.getMaxNumberOfSegmentsInCache());
        writer.setInt(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT,
                indexConfiguration.getMaxNumberOfKeysInSegment());
        writer.setInt(PROP_DISK_IO_BUFFER_SIZE_IN_BYTES,
                indexConfiguration.getDiskIoBufferSize());

        // Segment properties
        writer.setLong(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
                indexConfiguration.getMaxNumberOfKeysInSegmentCache());
        writer.setLong(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE,
                indexConfiguration.getMaxNumberOfKeysInSegmentWriteCache());
        writer.setLong(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING,
                indexConfiguration
                        .getMaxNumberOfKeysInSegmentCacheDuringFlushing());
        writer.setInt(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK,
                indexConfiguration.getMaxNumberOfKeysInSegmentChunk());
        final int threadCount = indexConfiguration.getNumberOfThreads() == null
                ? IndexConfigurationContract.NUMBER_OF_THREADS
                : indexConfiguration.getNumberOfThreads();
        writer.setInt(PROP_NUMBER_OF_THREADS, threadCount);
        final int ioThreadCount = indexConfiguration.getNumberOfIoThreads() == null
                ? IndexConfigurationContract.NUMBER_OF_IO_THREADS
                : indexConfiguration.getNumberOfIoThreads();
        writer.setInt(PROP_NUMBER_OF_IO_THREADS, ioThreadCount);

        // Segment bloom filter properties
        writer.setInt(PROP_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS,
                indexConfiguration.getBloomFilterNumberOfHashFunctions());
        writer.setInt(PROP_BLOOM_FILTER_INDEX_SIZE_IN_BYTES,
                indexConfiguration.getBloomFilterIndexSizeInBytes());
        if (indexConfiguration
                .getBloomFilterProbabilityOfFalsePositive() != null) {
            writer.setDouble(PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE,
                    indexConfiguration
                            .getBloomFilterProbabilityOfFalsePositive());
        } else {
            writer.setDouble(PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE,
                    BloomFilterBuilder.DEFAULT_PROBABILITY_OF_FALSE_POSITIVE);
        }
        writer.setString(PROP_ENCODING_CHUNK_FILTERS, serializeFilters(
                indexConfiguration.getEncodingChunkFilters()));

        writer.setString(PROP_DECODING_CHUNK_FILTERS, serializeFilters(
                indexConfiguration.getDecodingChunkFilters()));
        tx.close();
    }

    boolean exists() {
        return directoryFacade.isFileExistsAsync(CONFIGURATION_FILENAME)
                .toCompletableFuture().join();
    }

    @SuppressWarnings("unchecked")
    private <T> Class<T> toClass(final String className) {
        try {
            return (Class<T>) Class.forName(className);
        } catch (ClassNotFoundException ex) {
            throw new IllegalArgumentException(
                    "Unable to load class: " + className, ex);
        }
    }

    private List<ChunkFilter> parseFilterList(final String value) {
        return Arrays.stream(value.split(",")).map(String::trim)
                .filter(s -> !s.isEmpty()).map(this::instantiateFilter)
                .collect(Collectors.toList());
    }

    private String serializeFilters(final List<ChunkFilter> filters) {
        if (filters == null || filters.isEmpty()) {
            return "";
        }
        return filters.stream().map(filter -> filter.getClass().getName())
                .collect(Collectors.joining(","));
    }

    private ChunkFilter instantiateFilter(final String className) {
        final String requiredClassName = Vldtn.requireNonNull(className,
                "className");
        try {
            final Class<?> clazz = Class.forName(requiredClassName);
            if (!ChunkFilter.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException(String.format(
                        "Class '%s' does not implement ChunkFilter",
                        requiredClassName));
            }
            @SuppressWarnings("unchecked")
            final Class<? extends ChunkFilter> filterClass = (Class<? extends ChunkFilter>) clazz;
            return filterClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException ex) {
            throw new IllegalArgumentException(
                    String.format("Unable to instantiate chunk filter '%s'",
                            requiredClassName),
                    ex);
        }
    }

    private int getOrDefault(final PropertyView propsView, final String key,
            final int defaultValue) {
        final int value = propsView.getInt(key);
        if (value == 0) {
            return defaultValue;
        }
        return value;
    }

    private long getOrDefaultLong(final PropertyView propsView,
            final String key, final long defaultValue) {
        final long value = propsView.getLong(key);
        if (value == 0) {
            return defaultValue;
        }
        return value;
    }

}
