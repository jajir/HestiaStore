package org.hestiastore.index.bloomfilter;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.directory.Directory.Access;
import org.hestiastore.index.directory.DirectoryFacade;
import org.hestiastore.index.directory.FileWriter;
import org.hestiastore.index.directory.async.AsyncFileWriterBlockingAdapter;

public class BloomFilterWriter<K> extends AbstractCloseableResource {

    private final ConvertorToBytes<K> convertorToBytes;

    private final Hash hash;

    private final DirectoryFacade directoryFacade;
    private final String fileName;
    private final int diskIoBufferSize;

    BloomFilterWriter(final ConvertorToBytes<K> convertorToBytes,
            final Hash newHash, final DirectoryFacade directoryFacade,
            final String fileName, final int diskIoBufferSize) {
        this.convertorToBytes = Vldtn.requireNonNull(convertorToBytes,
                "convertorToBytes");
        this.hash = Vldtn.requireNonNull(newHash, "newHash");
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        this.diskIoBufferSize = diskIoBufferSize;
    }

    Hash getHashSnapshot() {
        return hash;
    }

    public boolean write(final K key) {
        Vldtn.requireNonNull(key, "key");
        return hash.store(convertorToBytes.toBytes(key));
    }

    @Override
    protected void doClose() {
        try (FileWriter writer = new AsyncFileWriterBlockingAdapter(
                directoryFacade
                        .getFileWriterAsync(fileName,
                                Access.OVERWRITE,
                                diskIoBufferSize)
                        .toCompletableFuture().join())) {
            writer.write(hash.getData());
        }
    }

}
