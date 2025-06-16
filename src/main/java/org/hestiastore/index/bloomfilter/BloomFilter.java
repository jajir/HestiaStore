package org.hestiastore.index.bloomfilter;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.directory.FileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BloomFilter<K> implements CloseableResource {

    private static final String TEMP_FILE_EXTENSION = ".tmp";

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

    public static <M> BloomFilterBuilder<M> builder() {
        return new BloomFilterBuilder<>();
    }

    BloomFilter(final Directory directory, final String bloomFilterFileName,
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
                final byte[] data = new byte[indexSizeInBytes];
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

    public BloomFilterWriter<K> openWriter() {
        return new BloomFilterWriter<>(convertorToBytes,
                new Hash(new BitArray(indexSizeInBytes), numberOfHashFunctions),
                this);
    }

    void setNewHash(final Hash newHash) {
        Vldtn.requireNonNull(newHash, "newHash");
        this.hash = newHash;
        try (FileWriter writer = directory.getFileWriter(getTempFileName(),
                Directory.Access.OVERWRITE, diskIoBufferSize)) {
            writer.write(hash.getData());
        }
        directory.renameFile(getTempFileName(), bloomFilterFileName);
    }

    private boolean isExists() {
        return directory.isFileExists(bloomFilterFileName);
    }

    private String getTempFileName() {
        return bloomFilterFileName + TEMP_FILE_EXTENSION;
    }

    /**
     * Get information if key is not stored in index. False doesn't mean that
     * key is stored in index it means that it's not sure.
     * 
     * @param key
     * @return Return <code>true</code> when it's sure that record is not stored
     *         in index. Otherwise return <code>false</false>
     */
    public boolean isNotStored(final K key) {
        if (hash == null) {
            bloomFilterStats.increment(false);
            return false;
        } else {
            final boolean out = hash.isNotStored(convertorToBytes.toBytes(key));
            bloomFilterStats.increment(out);
            return out;
        }
    }

    public BloomFilterStats getStatistics() {
        return bloomFilterStats;
    }

    public void incrementFalsePositive() {
        bloomFilterStats.incrementFalsePositive();
    }

    public long getNumberOfHashFunctions() {
        return numberOfHashFunctions;
    }

    public long getIndexSizeInBytes() {
        return indexSizeInBytes;
    }

    @Override
    public void close() {
        logger.debug("Closing bloom filter for '{}'. {}", relatedObjectName,
                bloomFilterStats.getStatsString());
    }

}
