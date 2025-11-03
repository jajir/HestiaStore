package org.hestiastore.index.bloomfilter;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.bytes.MutableBytes;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default on-disk backed Bloom filter implementation.
 *
 * @param <K> key type hashed by the filter
 */
final class BloomFilterImpl<K> extends AbstractCloseableResource
        implements BloomFilter<K> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Directory directory;

    private final String bloomFilterFileName;

    private final ConvertorToBytes<K> convertorToBytes;

    private final BloomFilterStats bloomFilterStats;

    private final int numberOfHashFunctions;

    private final int indexSizeInBytes;

    private final String relatedObjectName;

    private Hash hash;

    private final int diskIoBufferSize;

    BloomFilterImpl(final Directory directory, final String bloomFilterFileName,
            final int numberOfHashFunctions, final int indexSizeInBytes,
            final ConvertorToBytes<K> convertorToBytes,
            final String relatedObjectName, final int diskIoBufferSize) {
        this.directory = Vldtn.requireNonNull(directory, "diskIoBufferSize");
        this.bloomFilterFileName = Vldtn.requireNonNull(bloomFilterFileName,
                "bloomFilterFileName");
        this.convertorToBytes = Vldtn.requireNonNull(convertorToBytes,
                "convertorToBytes");
        this.relatedObjectName = Vldtn.requireNonNull(relatedObjectName,
                "relatedObjectName");
        this.indexSizeInBytes = indexSizeInBytes;
        this.numberOfHashFunctions = numberOfHashFunctions;
        this.bloomFilterStats = new BloomFilterStats();
        this.diskIoBufferSize = diskIoBufferSize;
        if (numberOfHashFunctions <= 0) {
            throw new IllegalArgumentException(
                    "Number of hash function cant be '0'");
        }
        if (isExists() && indexSizeInBytes > 0) {
            try (FileReader reader = directory
                    .getFileReader(bloomFilterFileName, diskIoBufferSize)) {
                final MutableBytes data = MutableBytes
                        .allocate(indexSizeInBytes);
                final int readed = reader.read(data);
                if (indexSizeInBytes != readed) {
                    throw new IllegalStateException(String.format(
                            "Bloom filter data from file '%s' wasn't loaded,"
                                    + " index expected size is '%s' but '%s' was loaded",
                            bloomFilterFileName, indexSizeInBytes, readed));
                }
                hash = new Hash(new BitArray(data), numberOfHashFunctions);
            }
        } else {
            hash = null;
        }
        logger.debug("Opening bloom filter for '{}'", relatedObjectName);
    }

    @Override
    public BloomFilterWriterTx<K> openWriteTx() {
        return new BloomFilterWriterTx<>(directory, bloomFilterFileName,
                convertorToBytes, numberOfHashFunctions, indexSizeInBytes,
                diskIoBufferSize, this);
    }

    @Override
    public void setNewHash(final Hash newHash) {
        Vldtn.requireNonNull(newHash, "newHash");
        this.hash = newHash;
    }

    private boolean isExists() {
        return directory.isFileExists(bloomFilterFileName);
    }

    @Override
    public boolean isNotStored(final K key) {
        if (hash == null) {
            bloomFilterStats.increment(false);
            return false;
        } else {
            final boolean out = hash
                    .isNotStored(convertorToBytes.toBytesBuffer(key));
            bloomFilterStats.increment(out);
            return out;
        }
    }

    @Override
    public BloomFilterStats getStatistics() {
        return bloomFilterStats;
    }

    @Override
    public void incrementFalsePositive() {
        bloomFilterStats.incrementFalsePositive();
    }

    @Override
    public long getNumberOfHashFunctions() {
        return numberOfHashFunctions;
    }

    @Override
    public long getIndexSizeInBytes() {
        return indexSizeInBytes;
    }

    @Override
    protected void doClose() {
        logger.debug("Closing bloom filter for '{}'. {}", relatedObjectName,
                bloomFilterStats.getStatsString());
    }
}
