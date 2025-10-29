package org.hestiastore.index.bloomfilter;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileWriter;

public class BloomFilterWriter<K> extends AbstractCloseableResource {

    private final ConvertorToBytes<K> convertorToBytes;

    private final Hash hash;

    private final Directory directory;
    private final String fileName;
    private final int diskIoBufferSize;

    BloomFilterWriter(final ConvertorToBytes<K> convertorToBytes,
            final Hash newHash, final Directory directory,
            final String fileName, final int diskIoBufferSize) {
        this.convertorToBytes = Vldtn.requireNonNull(convertorToBytes,
                "convertorToBytes");
        this.hash = Vldtn.requireNonNull(newHash, "newHash");
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        this.diskIoBufferSize = diskIoBufferSize;
    }

    Hash getHashSnapshot() {
        return hash;
    }

    public boolean write(final K key) {
        Vldtn.requireNonNull(key, "key");
        return hash.store(convertorToBytes.toBytesBuffer(key));
    }

    @Override
    protected void doClose() {
        try (FileWriter writer = directory.getFileWriter(fileName,
                Directory.Access.OVERWRITE, diskIoBufferSize)) {
            writer.write(hash.getData());
        }
    }

}
