package org.hestiastore.index.bloomfilter;

import org.hestiastore.index.GuardedWriteTransaction;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.ConvertorToBytes;

/**
 * Write transaction wrapper for bloom filter updates.
 */
public final class BloomFilterWriterTx<K>
        extends GuardedWriteTransaction<BloomFilterWriter<K>> {

    private final ConvertorToBytes<K> convertorToBytes;
    private final int numberOfHashFunctions;
    private final int indexSizeInBytes;
    private final BloomFilter<K> bloomFilter;

    BloomFilterWriterTx(final ConvertorToBytes<K> convertorToBytes,
            final int numberOfHashFunctions, final int indexSizeInBytes,
            final BloomFilter<K> bloomFilter) {
        this.convertorToBytes = Vldtn.requireNonNull(convertorToBytes,
                "convertorToBytes");
        this.numberOfHashFunctions = numberOfHashFunctions;
        this.indexSizeInBytes = indexSizeInBytes;
        this.bloomFilter = Vldtn.requireNonNull(bloomFilter, "bloomFilter");
    }

    @Override
    protected BloomFilterWriter<K> doOpen() {
        final Hash hash = new Hash(new BitArray(indexSizeInBytes),
                numberOfHashFunctions);
        return new BloomFilterWriter<>(convertorToBytes, hash, bloomFilter);
    }

    @Override
    protected void doCommit(final BloomFilterWriter<K> writer) {
        writer.close();
    }
}
