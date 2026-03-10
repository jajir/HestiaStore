package org.hestiastore.index.bloomfilter;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.EncodedBytes;
import org.hestiastore.index.datatype.TypeEncoder;
import org.hestiastore.index.directory.Directory;
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

    private static final TypeEncoder<Object> NO_OP_CONVERTOR = new TypeEncoder<Object>() {
        @Override
        public EncodedBytes encode(final Object value,
                final byte[] reusableBuffer) {
            final byte[] validatedBuffer = Vldtn.requireNonNull(reusableBuffer,
                    "reusableBuffer");
            byte[] output = validatedBuffer;
            if (output.length < 1) {
                output = new byte[1];
            }
            output[0] = 0;
            return new EncodedBytes(output, 1);
        }
    };

    private final BloomFilterStats stats = new BloomFilterStats();

    @Override
    public BloomFilterWriterTx<K> openWriteTx() {
        @SuppressWarnings("unchecked")
        final TypeEncoder<K> convertor = (TypeEncoder<K>) NO_OP_CONVERTOR;
        final Directory directory = new MemDirectory();
        return new BloomFilterWriterTx<>(directory, "bloomFilterNull",
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
