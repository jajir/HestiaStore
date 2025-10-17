package org.hestiastore.index.bloomfilter;

import org.hestiastore.index.CloseableResource;

/**
 * Abstraction for Bloom filter operations used across the index.
 *
 * @param <K> key type hashed by the filter
 */
public interface BloomFilter<K> extends CloseableResource {

    static <M> BloomFilterBuilder<M> builder() {
        return new BloomFilterBuilder<>();
    }

    /**
     * Create a writer that can add new keys into the filter.
     *
     * @return writer instance responsible for updating the backing structure
     */
    BloomFilterWriter<K> openWriter();

    /**
     * Determine whether the filter guarantees the key is absent.
     *
     * @param key key being probed
     * @return {@code true} when the filter guarantees the key is not present,
     *         {@code false} otherwise
     */
    boolean isNotStored(K key);

    /**
     * Retrieve statistics associated with the filter usage.
     *
     * @return non-null stats holder
     */
    BloomFilterStats getStatistics();

    /**
     * Mark that the filter produced a false positive response.
     */
    void incrementFalsePositive();

    /**
     * Provide the number of hash functions the filter uses.
     *
     * @return count of hash functions
     */
    long getNumberOfHashFunctions();

    /**
     * Report the size of the backing bit array.
     *
     * @return size expressed in bytes
     */
    long getIndexSizeInBytes();

    /**
     * Replace the internal hash state after write operations complete.
     *
     * @param newHash new hash snapshot to persist
     */
    void setNewHash(Hash newHash);
}
