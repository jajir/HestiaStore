package org.hestiastore.index.senku;

import java.util.function.ToIntFunction;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.senku.internal.SenkuRuntime;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfigurationDefaults;

/**
 * Flat builder for a new write-once Senku index.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SenkuIndexBuilder<K, V> {

    static final int MAX_SHARD_COUNT = 1_000_000;
    static final int MAX_IN_MEMORY_ENTRIES = 805_306_368;
    static final int DEFAULT_MAX_KEYS_PER_PAGE = 1_000_000;
    static final long DEFAULT_MAX_ENTRIES_PER_PART = 10_000_000L;
    static final int DEFAULT_MAINTENANCE_QUEUE_SIZE = 42;

    private final Directory directory;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final SenkuMergeFunctionRegistry<K, V> functions;

    private ToIntFunction<K> shardHashFunction;
    private Integer shardCount;
    private Integer maxInMemoryEntries;
    private int maxKeysPerPage = DEFAULT_MAX_KEYS_PER_PAGE;
    private Integer mergeFanIn;
    private Integer maintenanceThreads;
    private int maintenanceQueueSize = DEFAULT_MAINTENANCE_QUEUE_SIZE;
    private int diskIoBufferSize = IndexConfigurationDefaults.DEFAULT_DISK_IO_BUFFER_SIZE_BYTES;
    private long maxEntriesPerPart = DEFAULT_MAX_ENTRIES_PER_PART;

    SenkuIndexBuilder(final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final SenkuMergeFunctionRegistry<K, V> functions) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.functions = Vldtn.requireNonNull(functions, "functions");
    }

    /**
     * Sets the function used to select a fixed shard.
     *
     * @param value required hash function
     * @return this builder
     */
    public SenkuIndexBuilder<K, V> shardHashFunction(
            final ToIntFunction<K> value) {
        shardHashFunction = Vldtn.requireNonNull(value, "shardHashFunction");
        return this;
    }

    /**
     * Sets the fixed number of shards.
     *
     * @param value shard count from one through one million
     * @return this builder
     */
    public SenkuIndexBuilder<K, V> shardCount(final int value) {
        shardCount = Vldtn.requireBetween(value, 1, MAX_SHARD_COUNT,
                "shardCount");
        return this;
    }

    /**
     * Sets the distinct-key count per ingestion map that triggers a synchronous
     * flush. During a flush, one additional map can be populated.
     *
     * @param value in-memory entry limit
     * @return this builder
     */
    public SenkuIndexBuilder<K, V> maxInMemoryEntries(final int value) {
        maxInMemoryEntries = Vldtn.requireBetween(value, 1,
                MAX_IN_MEMORY_ENTRIES, "maxInMemoryEntries");
        return this;
    }

    /**
     * Sets the maximum number of entries encoded in one page.
     *
     * @param value positive page entry limit
     * @return this builder
     */
    public SenkuIndexBuilder<K, V> maxKeysPerPage(final int value) {
        maxKeysPerPage = Vldtn.requireGreaterThanZero(value,
                "maxKeysPerPage");
        return this;
    }

    /**
     * Sets the maximum number of sorted inputs consumed by one merge.
     *
     * @param value merge fan-in of at least two
     * @return this builder
     */
    public SenkuIndexBuilder<K, V> mergeFanIn(final int value) {
        mergeFanIn = Vldtn.requireBetween(value, 2, Integer.MAX_VALUE,
                "mergeFanIn");
        return this;
    }

    /**
     * Sets the maximum number of parallel maintenance jobs.
     *
     * @param value positive worker count
     * @return this builder
     */
    public SenkuIndexBuilder<K, V> maintenanceThreads(final int value) {
        maintenanceThreads = Vldtn.requireGreaterThanZero(value,
                "maintenanceThreads");
        return this;
    }

    /**
     * Sets the bounded maintenance executor queue size.
     *
     * @param value positive queue size
     * @return this builder
     */
    public SenkuIndexBuilder<K, V> maintenanceQueueSize(final int value) {
        maintenanceQueueSize = Vldtn.requireGreaterThanZero(value,
                "maintenanceQueueSize");
        return this;
    }

    /**
     * Sets the chunk-store disk I/O buffer size.
     *
     * @param value positive size divisible by 1024
     * @return this builder
     */
    public SenkuIndexBuilder<K, V> diskIoBufferSize(final int value) {
        diskIoBufferSize = Vldtn.requireIoBufferSize(value,
                "diskIoBufferSize");
        return this;
    }

    /**
     * Sets the maximum number of entries in one physical large-file part.
     *
     * @param value positive part entry limit
     * @return this builder
     */
    public SenkuIndexBuilder<K, V> maxEntriesPerPart(final long value) {
        maxEntriesPerPart = Vldtn.requireGreaterThanZero(value,
                "maxEntriesPerPart");
        return this;
    }

    /**
     * Validates the complete configuration and creates the writing handle.
     *
     * @return exclusive writing handle
     */
    public SenkuWriting<K, V> create() {
        validate();
        final SenkuWriting<K, V> writing = SenkuRuntime.create(directory,
                keyTypeDescriptor, valueTypeDescriptor,
                functions.requireFunction(), shardHashFunction, shardCount,
                maxInMemoryEntries, initialMapCapacity(), maxKeysPerPage,
                mergeFanIn, maintenanceThreads, maintenanceQueueSize,
                diskIoBufferSize, maxEntriesPerPart);
        functions.freeze();
        return writing;
    }

    void validate() {
        Vldtn.requireNonNull(shardHashFunction, "shardHashFunction");
        Vldtn.requireNonNull(shardCount, "shardCount");
        Vldtn.requireNonNull(maxInMemoryEntries, "maxInMemoryEntries");
        Vldtn.requireNonNull(mergeFanIn, "mergeFanIn");
        Vldtn.requireNonNull(maintenanceThreads, "maintenanceThreads");
        Vldtn.requireTrue(maxEntriesPerPart >= maxKeysPerPage,
                "Property 'maxEntriesPerPart' must be greater than or equal to maxKeysPerPage");
        functions.requireFunction();
        initialMapCapacity();
        shardIndexBytes();
    }

    int initialMapCapacity() {
        final int entryLimit = Vldtn.requireNonNull(maxInMemoryEntries,
                "maxInMemoryEntries");
        final long requestedCapacity = (4L * entryLimit + 2L) / 3L;
        Vldtn.requireTrue(requestedCapacity <= 1L << 30,
                "Property 'maxInMemoryEntries' exceeds HashMap capacity");
        return (int) requestedCapacity;
    }

    int shardIndexBytes() {
        final int configuredShardCount = Vldtn.requireNonNull(shardCount,
                "shardCount");
        final long byteCount = 20L * configuredShardCount;
        Vldtn.requireTrue(byteCount <= Integer.MAX_VALUE,
                "Property 'shardCount' creates an oversized shard index");
        return (int) byteCount;
    }

    Directory directory() {
        return directory;
    }

    TypeDescriptor<K> keyTypeDescriptor() {
        return keyTypeDescriptor;
    }

    TypeDescriptor<V> valueTypeDescriptor() {
        return valueTypeDescriptor;
    }

    SenkuMergeFunction<K, V> mergeFunction() {
        return functions.requireFunction();
    }

    ToIntFunction<K> shardHashFunction() {
        return shardHashFunction;
    }

    int shardCount() {
        return shardCount;
    }

    int maxInMemoryEntries() {
        return maxInMemoryEntries;
    }

    int maxKeysPerPage() {
        return maxKeysPerPage;
    }

    int mergeFanIn() {
        return mergeFanIn;
    }

    int maintenanceThreads() {
        return maintenanceThreads;
    }

    int maintenanceQueueSize() {
        return maintenanceQueueSize;
    }

    int diskIoBufferSize() {
        return diskIoBufferSize;
    }

    long maxEntriesPerPart() {
        return maxEntriesPerPart;
    }
}
