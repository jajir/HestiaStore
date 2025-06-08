package org.hestiastore.index.bloomfilter;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.ConvertorToBytes;

public class BloomFilterWriter<K> implements CloseableResource {

    private final ConvertorToBytes<K> convertorToBytes;

    private final Hash hash;

    private final BloomFilter<K> bloomFilter;

    BloomFilterWriter(final ConvertorToBytes<K> convertorToBytes,
            final Hash newHash, final BloomFilter<K> bloomFilter) {
        this.convertorToBytes = Vldtn.requireNonNull(convertorToBytes,
                "convertorToBytes");
        this.hash = Vldtn.requireNonNull(newHash, "newHash");
        this.bloomFilter = Vldtn.requireNonNull(bloomFilter, "bloomFilter");
    }

    public boolean write(final K key) {
        Vldtn.requireNonNull(key, "key");
        return hash.store(convertorToBytes.toBytes(key));
    }

    @Override
    public void close() {
        bloomFilter.setNewHash(hash);
    }

}
