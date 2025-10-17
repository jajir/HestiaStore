package org.hestiastore.index.bloomfilter;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.directory.Directory;

public class BloomFilterBuilder<K> {

    public static final double DEFAULT_PROBABILITY_OF_FALSE_POSITIVE = 0.01;
    private static final int DEFAULT_DISK_IO_BUFFER_SIZE = 2 * 1024;

    private Directory directory;
    private String bloomFilterFileName;
    private ConvertorToBytes<K> convertorToBytes;
    private Long numberOfKeys = null;
    private Integer numberOfHashFunctions = null;
    private Integer indexSizeInBytes = null;
    private double probabilityOfFalsePositive = DEFAULT_PROBABILITY_OF_FALSE_POSITIVE;
    private String relatedObjectName;
    private int diskIoBufferSize = DEFAULT_DISK_IO_BUFFER_SIZE;

    BloomFilterBuilder() {

    }

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
            final Integer numberOfHashFunctions) {
        this.numberOfHashFunctions = numberOfHashFunctions;
        return this;
    }

    public BloomFilterBuilder<K> withIndexSizeInBytes(
            final Integer indexSizeInBytes) {
        this.indexSizeInBytes = indexSizeInBytes;
        return this;
    }

    public BloomFilterBuilder<K> withConvertorToBytes(
            final ConvertorToBytes<K> convertorToBytes) {
        this.convertorToBytes = convertorToBytes;
        return this;
    }

    public BloomFilterBuilder<K> withNumberOfKeys(final Long numberOfKeys) {
        this.numberOfKeys = numberOfKeys;
        return this;
    }

    public BloomFilterBuilder<K> withProbabilityOfFalsePositive(
            final Double probabilityOfFalsePositive) {
        if (probabilityOfFalsePositive != null) {
            this.probabilityOfFalsePositive = probabilityOfFalsePositive;
        }
        return this;
    }

    public BloomFilterBuilder<K> withRelatedObjectName(
            final String relatedObjectName) {
        this.relatedObjectName = relatedObjectName;
        return this;
    }

    public BloomFilterBuilder<K> withDiskIoBufferSize(
            final int diskIoBufferSize) {
        this.diskIoBufferSize = diskIoBufferSize;
        return this;
    }

    public BloomFilter<K> build() {
        Vldtn.requireNonNull(directory, "directory");
        Vldtn.requireNonNull(bloomFilterFileName, "bloomFilterFileName");
        Vldtn.requireNonNull(convertorToBytes, "convertorToBytes");
        if (numberOfKeys == null && indexSizeInBytes == null) {
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
        if (indexSizeInBytes == null) {
            indexSizeInBytes = -(int) (numberOfKeys
                    * Math.log(probabilityOfFalsePositive)
                    / Math.pow(Math.log(2), 2));
        }
        if (numberOfHashFunctions == null) {
            if (numberOfKeys == null || numberOfKeys == 1) {
                numberOfHashFunctions = 1;
            } else {
                numberOfHashFunctions = (int) Math.ceil(
                        indexSizeInBytes / (double) numberOfKeys * Math.log(2));
            }
        }
        return new BloomFilterImpl<>(directory, bloomFilterFileName,
                numberOfHashFunctions, indexSizeInBytes, convertorToBytes,
                relatedObjectName, diskIoBufferSize);
    }

}
