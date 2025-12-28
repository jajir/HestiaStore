package org.hestiastore.index.bloomfilter;

import org.hestiastore.index.GuardedWriteTransaction;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.directory.async.AsyncDirectory;

/**
 * Write transaction wrapper for bloom filter updates.
 */
public final class BloomFilterWriterTx<K>
        extends GuardedWriteTransaction<BloomFilterWriter<K>> {

    private static final String TEMP_FILE_EXTENSION = ".tmp";

    private final AsyncDirectory directoryFacade;
    private final String bloomFilterFileName;
    private final ConvertorToBytes<K> convertorToBytes;
    private final int numberOfHashFunctions;
    private final int indexSizeInBytes;
    private final int diskIoBufferSize;
    private final BloomFilter<K> bloomFilter;

    BloomFilterWriterTx(final AsyncDirectory directoryFacade,
            final String bloomFilterFileName,
            final ConvertorToBytes<K> convertorToBytes,
            final int numberOfHashFunctions, final int indexSizeInBytes,
            final int diskIoBufferSize, final BloomFilter<K> bloomFilter) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.bloomFilterFileName = Vldtn.requireNonNull(bloomFilterFileName,
                "bloomFilterFileName");
        this.convertorToBytes = Vldtn.requireNonNull(convertorToBytes,
                "convertorToBytes");
        this.numberOfHashFunctions = numberOfHashFunctions;
        this.indexSizeInBytes = indexSizeInBytes;
        this.diskIoBufferSize = diskIoBufferSize;
        this.bloomFilter = Vldtn.requireNonNull(bloomFilter, "bloomFilter");
    }

    @Override
    protected BloomFilterWriter<K> doOpen() {
        final Hash hash = new Hash(new BitArray(indexSizeInBytes),
                numberOfHashFunctions);
        return new BloomFilterWriter<>(convertorToBytes, hash, directoryFacade,
                getTempFileName(), diskIoBufferSize);
    }

    @Override
    protected void doCommit(final BloomFilterWriter<K> writer) {
        directoryFacade.renameFileAsync(getTempFileName(), bloomFilterFileName)
                .toCompletableFuture().join();
        bloomFilter.setNewHash(writer.getHashSnapshot());
    }

    private String getTempFileName() {
        return bloomFilterFileName + TEMP_FILE_EXTENSION;
    }

}
