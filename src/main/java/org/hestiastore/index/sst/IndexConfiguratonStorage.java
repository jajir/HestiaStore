package org.hestiastore.index.sst;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bloomfilter.BloomFilterBuilder;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.Props;

public class IndexConfiguratonStorage<K, V> {

    private static final String PROP_KEY_CLASS = "keyClass";
    private static final String PROP_VALUE_CLASS = "valueClass";
    private static final String PROP_KEY_TYPE_DESCRIPTOR = "keyTypeDescriptor";
    private static final String PROP_VALUE_TYPE_DESCRIPTOR = "valueTypeDescriptor";
    private static final String PROP_INDEX_NAME = "indexName";
    private static final String PROP_USE_FULL_LOG = "logEnabled";
    private static final String PROP_IS_THREAD_SAFE = "isThreadSafe";

    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE = "maxNumberOfKeysInSegmentCache";
    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING = "maxNumberOfKeysInSegmentCacheDuringFlushing";
    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK = "maxNumberOfKeysInSegmentChunk";
    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_CACHE = "maxNumberOfKeysInCache";
    private static final String PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT = "maxNumberOfKeysInSegment";
    private static final String PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE = "maxNumberOfSegmentsInCache";
    private static final String PROP_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS = "bloomFilterNumberOfHashFunctions";
    private static final String PROP_BLOOM_FILTER_INDEX_SIZE_IN_BYTES = "bloomFilterIndexSizeInBytes";
    private static final String PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE = "bloomFilterProbabilityOfFalsePositive";
    private static final String PROP_DISK_IO_BUFFER_SIZE_IN_BYTES = "diskIoBufferSizeInBytes";
    private static final String PROP_ENCODING_CHUNK_FILTERS = "encodingChunkFilters";
    private static final String PROP_DECODING_CHUNK_FILTERS = "decodingChunkFilters";

    private static final String CONFIGURATION_FILENAME = "index-configuration.properties";

    private final Directory directory;

    IndexConfiguratonStorage(final Directory directory) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
    }

    IndexConfiguration<K, V> load() {
        final Props props = new Props(directory, CONFIGURATION_FILENAME, true);
        final Class<K> keyClass = toClass(props.getString(PROP_KEY_CLASS));
        final Class<V> valueClass = toClass(props.getString(PROP_VALUE_CLASS));
        final IndexConfigurationBuilder<K, V> builder = IndexConfiguration
                .<K, V>builder()//
                .withKeyClass(keyClass) //
                .withValueClass(valueClass)//
                .withName(props.getString(PROP_INDEX_NAME))//
                .withLogEnabled(props.getBoolean(PROP_USE_FULL_LOG))//
                .withThreadSafe(props.getBoolean(PROP_IS_THREAD_SAFE))//

                // Index runtime properties
                .withMaxNumberOfKeysInCache(
                        props.getInt(PROP_MAX_NUMBER_OF_KEYS_IN_CACHE))//
                .withMaxNumberOfSegmentsInCache(
                        props.getInt(PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE))//
                .withMaxNumberOfKeysInSegment(
                        props.getInt(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT))//
                .withDiskIoBufferSizeInBytes(
                        props.getInt(PROP_DISK_IO_BUFFER_SIZE_IN_BYTES))//

                // Segment properties
                .withMaxNumberOfKeysInSegmentCache(
                        props.getLong(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE))//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(props.getLong(
                        PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING))//
                .withMaxNumberOfKeysInSegmentChunk(
                        props.getInt(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK))//

                // Segment bloom filter properties
                .withBloomFilterNumberOfHashFunctions(props
                        .getInt(PROP_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS))//
                .withBloomFilterIndexSizeInBytes(
                        props.getInt(PROP_BLOOM_FILTER_INDEX_SIZE_IN_BYTES))//
        ;

        if (props.getDouble(
                PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE) != 0) {
            builder.withBloomFilterProbabilityOfFalsePositive(props.getDouble(
                    PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE));
        }

        builder.withKeyTypeDescriptor(
                props.getString(PROP_KEY_TYPE_DESCRIPTOR));
        builder.withValueTypeDescriptor(
                props.getString(PROP_VALUE_TYPE_DESCRIPTOR));

        final String encodingFilters = props
                .getString(PROP_ENCODING_CHUNK_FILTERS);
        if (encodingFilters != null && !encodingFilters.isBlank()) {
            builder.withEncodingFilters(parseFilterList(encodingFilters));
        }

        final String decodingFilters = props
                .getString(PROP_DECODING_CHUNK_FILTERS);
        if (decodingFilters != null && !decodingFilters.isBlank()) {
            builder.withDecodingFilters(parseFilterList(decodingFilters));
        }

        return builder.build();
    }

    public void save(IndexConfiguration<K, V> indexConfiguration) {
        final Props props = new Props(directory, CONFIGURATION_FILENAME);
        props.setString(PROP_KEY_CLASS,
                indexConfiguration.getKeyClass().getName());
        props.setString(PROP_VALUE_CLASS,
                indexConfiguration.getValueClass().getName());
        props.setString(PROP_KEY_TYPE_DESCRIPTOR,
                indexConfiguration.getKeyTypeDescriptor());
        props.setString(PROP_VALUE_TYPE_DESCRIPTOR,
                indexConfiguration.getValueTypeDescriptor());
        props.setString(PROP_INDEX_NAME, indexConfiguration.getIndexName());
        props.setBoolean(PROP_USE_FULL_LOG, indexConfiguration.isLogEnabled());
        props.setBoolean(PROP_IS_THREAD_SAFE,
                indexConfiguration.isThreadSafe());

        // Index runtime properties
        props.setLong(PROP_MAX_NUMBER_OF_KEYS_IN_CACHE,
                indexConfiguration.getMaxNumberOfKeysInCache());
        props.setInt(PROP_MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                indexConfiguration.getMaxNumberOfSegmentsInCache());
        props.setInt(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT,
                indexConfiguration.getMaxNumberOfKeysInSegment());
        props.setInt(PROP_DISK_IO_BUFFER_SIZE_IN_BYTES,
                indexConfiguration.getDiskIoBufferSize());

        // Segment properties
        props.setLong(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
                indexConfiguration.getMaxNumberOfKeysInSegmentCache());
        props.setLong(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING,
                indexConfiguration
                        .getMaxNumberOfKeysInSegmentCacheDuringFlushing());
        props.setInt(PROP_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK,
                indexConfiguration.getMaxNumberOfKeysInSegmentChunk());

        // Segment bloom filter properties
        props.setInt(PROP_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS,
                indexConfiguration.getBloomFilterNumberOfHashFunctions());
        props.setInt(PROP_BLOOM_FILTER_INDEX_SIZE_IN_BYTES,
                indexConfiguration.getBloomFilterIndexSizeInBytes());
        if (indexConfiguration
                .getBloomFilterProbabilityOfFalsePositive() != null) {
            props.setDouble(PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE,
                    indexConfiguration
                            .getBloomFilterProbabilityOfFalsePositive());
        } else {
            props.setDouble(PROP_BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE,
                    BloomFilterBuilder.DEFAULT_PROBABILITY_OF_FALSE_POSITIVE);
        }
        props.setString(PROP_ENCODING_CHUNK_FILTERS,
                serializeFilters(indexConfiguration.getEncodingChunkFilters()));
        props.setString(PROP_DECODING_CHUNK_FILTERS,
                serializeFilters(indexConfiguration.getDecodingChunkFilters()));
        props.writeData();
    }

    boolean exists() {
        return directory.isFileExists(CONFIGURATION_FILENAME);
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
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(this::instantiateFilter)
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
            throw new IllegalArgumentException(String.format(
                    "Unable to instantiate chunk filter '%s'",
                    requiredClassName), ex);
        }
    }

}
