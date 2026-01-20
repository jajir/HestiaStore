package org.hestiastore.index.bloomfilter;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.directory.async.AsyncDirectory;

/**
 * Builder for {@link BloomFilter} instances.
 *
 * <p>
 * The builder accepts optional sizing inputs. When neither the expected number
 * of keys nor the index size is configured, {@link #build()} returns a
 * {@link BloomFilterNull} instance.
 * </p>
 *
 * <p>
 * Sentinel values (for example {@code -1}) are treated as "unset" to allow
 * callers to pass through configuration defaults without additional checks.
 * </p>
 *
 * @param <K> key type hashed by the filter
 */
public class BloomFilterBuilder<K> {

    /**
     * Default probability of false positives when not configured explicitly.
     */
    public static final double DEFAULT_PROBABILITY_OF_FALSE_POSITIVE = 0.01;

    /**
     * Sentinel value marking an unset hash function count.
     */
    public static final int UNSET_NUMBER_OF_HASH_FUNCTIONS = -1;

    /**
     * Sentinel value marking an unset index size in bytes.
     */
    public static final int UNSET_INDEX_SIZE_IN_BYTES = -1;

    /**
     * Sentinel value marking an unset key count.
     */
    public static final long UNSET_NUMBER_OF_KEYS = -1L;

    /**
     * Sentinel value marking an unset false-positive probability.
     */
    public static final double UNSET_PROBABILITY_OF_FALSE_POSITIVE = -1D;

    private static final int DEFAULT_DISK_IO_BUFFER_SIZE = 2 * 1024;

    private AsyncDirectory directoryFacade;
    private String bloomFilterFileName;
    private ConvertorToBytes<K> convertorToBytes;
    private long numberOfKeys = UNSET_NUMBER_OF_KEYS;
    private int numberOfHashFunctions = UNSET_NUMBER_OF_HASH_FUNCTIONS;
    private int indexSizeInBytes = UNSET_INDEX_SIZE_IN_BYTES;
    private double probabilityOfFalsePositive = DEFAULT_PROBABILITY_OF_FALSE_POSITIVE;
    private String relatedObjectName;
    private int diskIoBufferSize = DEFAULT_DISK_IO_BUFFER_SIZE;

    BloomFilterBuilder() {

    }

    /**
     * Sets the async directory backing the bloom filter storage.
     *
     * @param directoryFacade async directory for bloom filter data
     * @return this builder
     */
    public BloomFilterBuilder<K> withAsyncDirectory(
            final AsyncDirectory directoryFacade) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        return this;
    }

    /**
     * Sets the file name used for the bloom filter data.
     *
     * @param bloomFilterFileName bloom filter file name
     * @return this builder
     */
    public BloomFilterBuilder<K> withBloomFilterFileName(
            final String bloomFilterFileName) {
        this.bloomFilterFileName = bloomFilterFileName;
        return this;
    }

    /**
     * Sets the number of hash functions to use.
     *
     * @param numberOfHashFunctions hash function count, or
     *        {@link #UNSET_NUMBER_OF_HASH_FUNCTIONS} to keep it unset
     * @return this builder
     */
    public BloomFilterBuilder<K> withNumberOfHashFunctions(
            final int numberOfHashFunctions) {
        if (numberOfHashFunctions == UNSET_NUMBER_OF_HASH_FUNCTIONS) {
            this.numberOfHashFunctions = UNSET_NUMBER_OF_HASH_FUNCTIONS;
            return this;
        }
        this.numberOfHashFunctions = numberOfHashFunctions;
        return this;
    }

    /**
     * Sets the size of the bloom filter index in bytes.
     *
     * @param indexSizeInBytes index size in bytes, or
     *        {@link #UNSET_INDEX_SIZE_IN_BYTES} to keep it unset
     * @return this builder
     */
    public BloomFilterBuilder<K> withIndexSizeInBytes(
            final int indexSizeInBytes) {
        if (indexSizeInBytes == UNSET_INDEX_SIZE_IN_BYTES) {
            this.indexSizeInBytes = UNSET_INDEX_SIZE_IN_BYTES;
            return this;
        }
        this.indexSizeInBytes = indexSizeInBytes;
        return this;
    }

    /**
     * Sets the key-to-bytes convertor used by the bloom filter.
     *
     * @param convertorToBytes key convertor
     * @return this builder
     */
    public BloomFilterBuilder<K> withConvertorToBytes(
            final ConvertorToBytes<K> convertorToBytes) {
        this.convertorToBytes = convertorToBytes;
        return this;
    }

    /**
     * Sets the expected number of keys stored in the filter.
     *
     * @param numberOfKeys expected key count, or {@link #UNSET_NUMBER_OF_KEYS}
     *        to leave it unset
     * @return this builder
     */
    public BloomFilterBuilder<K> withNumberOfKeys(final long numberOfKeys) {
        if (numberOfKeys == UNSET_NUMBER_OF_KEYS) {
            this.numberOfKeys = UNSET_NUMBER_OF_KEYS;
            return this;
        }
        this.numberOfKeys = numberOfKeys;
        return this;
    }

    /**
     * Sets the probability of false positives.
     *
     * @param probabilityOfFalsePositive probability in range (0,1], or
     *        {@link #UNSET_PROBABILITY_OF_FALSE_POSITIVE} to keep the default
     * @return this builder
     */
    public BloomFilterBuilder<K> withProbabilityOfFalsePositive(
            final double probabilityOfFalsePositive) {
        if (Double.compare(probabilityOfFalsePositive,
                UNSET_PROBABILITY_OF_FALSE_POSITIVE) == 0) {
            this.probabilityOfFalsePositive = DEFAULT_PROBABILITY_OF_FALSE_POSITIVE;
            return this;
        }
        this.probabilityOfFalsePositive = probabilityOfFalsePositive;
        return this;
    }

    /**
     * Sets the related object name for logging and diagnostics.
     *
     * @param relatedObjectName related object name
     * @return this builder
     */
    public BloomFilterBuilder<K> withRelatedObjectName(
            final String relatedObjectName) {
        this.relatedObjectName = relatedObjectName;
        return this;
    }

    /**
     * Sets the disk I/O buffer size used when reading/writing filter data.
     *
     * @param diskIoBufferSize buffer size in bytes
     * @return this builder
     */
    public BloomFilterBuilder<K> withDiskIoBufferSize(
            final int diskIoBufferSize) {
        this.diskIoBufferSize = diskIoBufferSize;
        return this;
    }

    /**
     * Builds the bloom filter instance.
     *
     * @return bloom filter or {@link BloomFilterNull} when sizing info is missing
     * @throws IllegalStateException when the false-positive probability is out
     *         of range
     */
    public BloomFilter<K> build() {
        Vldtn.requireNonNull(directoryFacade, "directoryFacade");
        Vldtn.requireNonNull(bloomFilterFileName, "bloomFilterFileName");
        Vldtn.requireNonNull(convertorToBytes, "convertorToBytes");
        if (numberOfKeys == UNSET_NUMBER_OF_KEYS
                && indexSizeInBytes == UNSET_INDEX_SIZE_IN_BYTES) {
            return new BloomFilterNull<>();
        }
        if (probabilityOfFalsePositive <= 0) {
            throw new IllegalStateException(
                    "Probability of false positive must be greater than zero.");
        }
        if (probabilityOfFalsePositive > 1) {
            throw new IllegalStateException(
                    "Probability of false positive must be less than one or equal to one.");
        }
        if (indexSizeInBytes == UNSET_INDEX_SIZE_IN_BYTES) {
            indexSizeInBytes = -(int) (numberOfKeys
                    * Math.log(probabilityOfFalsePositive)
                    / Math.pow(Math.log(2), 2));
        }
        if (numberOfHashFunctions == UNSET_NUMBER_OF_HASH_FUNCTIONS) {
            if (numberOfKeys == UNSET_NUMBER_OF_KEYS || numberOfKeys == 1) {
                numberOfHashFunctions = 1;
            } else {
                numberOfHashFunctions = (int) Math.ceil(
                        indexSizeInBytes / (double) numberOfKeys * Math.log(2));
            }
        }
        return new BloomFilterImpl<>(directoryFacade, bloomFilterFileName,
                numberOfHashFunctions, indexSizeInBytes, convertorToBytes,
                relatedObjectName, diskIoBufferSize);
    }

}
