package org.hestiastore.index.bloomfilter;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.MemDirectory;

/**
 * No-op implementation of {@link BloomFilter}. It allows callers to rely on a
 * harmless Bloom filter when sizing information is unavailable. All
 * interactions stay in-memory and never touch underlying storage.
 *
 * @param <K> key type hashed by the filter
 */
public final class BloomFilterNull<K> extends AbstractCloseableResource
        implements BloomFilter<K> {

    private static final ConvertorToBytes<Object> NO_OP_CONVERTOR = value -> new byte[] {
            0 };

    private final BloomFilterStats stats = new BloomFilterStats();

    @Override
    public BloomFilterWriterTx<K> openWriteTx() {
        @SuppressWarnings("unchecked")
        final ConvertorToBytes<K> convertor = (ConvertorToBytes<K>) NO_OP_CONVERTOR;
        final AsyncDirectory asyncDirectory = org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                .wrap(new MemDirectory());
        return new BloomFilterWriterTx<>(asyncDirectory, "bloomFilterNull",
                convertor, 1, 1, 1, this);
    }

    @Override
    public boolean isNotStored(final K key) {
        // Behave as if the value might exist; calling code can fall back to
        // index.
        return false;
    }

    @Override
    public BloomFilterStats getStatistics() {
        return stats;
    }

    @Override
    public void incrementFalsePositive() {
        // No tracking needed for the null implementation.
    }

    @Override
    public long getNumberOfHashFunctions() {
        return 0;
    }

    @Override
    public long getIndexSizeInBytes() {
        return 0;
    }

    @Override
    public void setNewHash(final Hash newHash) {
        // Ignore updates; there is no persisted backing store.
    }

    @Override
    protected void doClose() {
        // Nothing to close.
    }
}
