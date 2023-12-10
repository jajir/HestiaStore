package com.coroptis.index.bloomfilter;

import com.coroptis.index.datatype.ConvertorToBytes;
import com.coroptis.index.directory.Directory;

public class BloomFilterBuilder<K> {
    private Directory directory;
    private String bloomFilterFileName;
    private int numberOfHashFunctions;
    private int indexSizeInBytes;
    private ConvertorToBytes<K> convertorToBytes;

    public BloomFilterBuilder<K> withDirectory(final Directory directory) {
        this.directory = directory;
        return this;
    }

    public BloomFilterBuilder<K> withBloomFilterFileName(
            final String bloomFilterFileName) {
        this.bloomFilterFileName = bloomFilterFileName;
        return this;
    }

    public BloomFilterBuilder<K> withNumberOfHashFunctions(
            final int numberOfHashFunctions) {
        this.numberOfHashFunctions = numberOfHashFunctions;
        return this;
    }

    public BloomFilterBuilder<K> withIndexSizeInBytes(
            final int indexSizeInBytes) {
        this.indexSizeInBytes = indexSizeInBytes;
        return this;
    }

    public BloomFilterBuilder<K> withConvertorToBytes(
            final ConvertorToBytes<K> convertorToBytes) {
        this.convertorToBytes = convertorToBytes;
        return this;
    }

    public BloomFilter<K> build() {
        return new BloomFilter<>(directory, bloomFilterFileName,
                numberOfHashFunctions, indexSizeInBytes, convertorToBytes);
    }

}
