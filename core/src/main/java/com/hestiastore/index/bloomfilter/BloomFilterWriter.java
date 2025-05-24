package com.hestiastore.index.bloomfilter;

import java.util.Objects;

import com.hestiastore.index.CloseableResource;
import com.hestiastore.index.datatype.ConvertorToBytes;

public class BloomFilterWriter<K> implements CloseableResource {

    private final ConvertorToBytes<K> convertorToBytes;

    private final Hash hash;

    private final BloomFilter<K> bloomFilter;

    BloomFilterWriter(final ConvertorToBytes<K> convertorToBytes,
            final Hash newHash, final BloomFilter<K> bloomFilter) {
        this.convertorToBytes = Objects.requireNonNull(convertorToBytes);
        this.hash = Objects.requireNonNull(newHash);
        this.bloomFilter = Objects.requireNonNull(bloomFilter);
    }

    public boolean write(final K key) {
        Objects.requireNonNull(key, "Key can't be null");
        return hash.store(convertorToBytes.toBytes(key));
    }

    @Override
    public void close() {
        bloomFilter.setNewHash(hash);
    }

}
