package org.hestiastore.index.bloomfilter;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeEncoder;
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

    private static final int INITIAL_REUSABLE_BUFFER_SIZE = 64;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Directory directoryFacade;

    private final String bloomFilterFileName;

    private final TypeEncoder<K> convertorToBytes;

    private final BloomFilterStats bloomFilterStats;

    private final int numberOfHashFunctions;

    private final int indexSizeInBytes;

    private final String relatedObjectName;

    private Hash hash;

    private final int diskIoBufferSize;

    private final ThreadLocal<byte[]> reusableBytesBuffer = ThreadLocal
            .withInitial(() -> new byte[INITIAL_REUSABLE_BUFFER_SIZE]);

    BloomFilterImpl(final Directory directoryFacade,
            final String bloomFilterFileName,
            final int numberOfHashFunctions, final int indexSizeInBytes,
            final TypeEncoder<K> convertorToBytes,
            final String relatedObjectName, final int diskIoBufferSize) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
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
        Vldtn.requireGreaterThanZero(numberOfHashFunctions,
                "numberOfHashFunctions");
        hash = loadHashIfPresent();
        logger.debug("Opening bloom filter for '{}'", relatedObjectName);
    }

    @Override
    public BloomFilterWriterTx<K> openWriteTx() {
        return new BloomFilterWriterTx<>(directoryFacade, bloomFilterFileName,
                convertorToBytes, numberOfHashFunctions, indexSizeInBytes,
                diskIoBufferSize, this);
    }

    @Override
    public void setNewHash(final Hash newHash) {
        Vldtn.requireNonNull(newHash, "newHash");
        this.hash = newHash;
    }

    private boolean isExists() {
        return directoryFacade.isFileExists(bloomFilterFileName);
    }

    private Hash loadHashIfPresent() {
        if (!isExists() || indexSizeInBytes <= 0) {
            return null;
        }
        try (FileReader reader = directoryFacade.getFileReader(
                bloomFilterFileName, diskIoBufferSize)) {
            final byte[] data = new byte[indexSizeInBytes];
            final int readed = reader.read(data);
            if (indexSizeInBytes != readed) {
                throw new IllegalStateException(String.format(
                        "Bloom filter data from file '%s' wasn't loaded,"
                                + " index expected size is '%s' but '%s' was loaded",
                        bloomFilterFileName, indexSizeInBytes, readed));
            }
            return new Hash(new BitArray(data), numberOfHashFunctions);
        } catch (final RuntimeException e) {
            if (!isExists()) {
                return null;
            }
            throw e;
        }
    }

    @Override
    public boolean isNotStored(final K key) {
        if (hash == null) {
            bloomFilterStats.increment(false);
            return false;
        } else {
            final boolean out = isNotStoredInternal(key);
            bloomFilterStats.increment(out);
            return out;
        }
    }

    private boolean isNotStoredInternal(final K key) {
        final int bytesLength = convertorToBytes.bytesLength(key);
        if (bytesLength <= 0) {
            return hash.isNotStored(TypeEncoder.toByteArray(convertorToBytes,
                    key));
        }
        byte[] buffer = reusableBytesBuffer.get();
        if (buffer.length < bytesLength) {
            buffer = new byte[bytesLength];
            reusableBytesBuffer.set(buffer);
        }
        final int writtenBytes = convertorToBytes.toBytes(key, buffer);
        return hash.isNotStored(buffer, writtenBytes);
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
