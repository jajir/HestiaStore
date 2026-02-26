package org.hestiastore.index.bloomfilter;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeEncoder;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.Directory.Access;
import org.hestiastore.index.directory.FileWriter;

public class BloomFilterWriter<K> extends AbstractCloseableResource {

    private final TypeEncoder<K> convertorToBytes;

    private final Hash hash;

    private final Directory directoryFacade;
    private final String fileName;
    private final int diskIoBufferSize;
    private byte[] encodedKeyBuffer = new byte[0];

    BloomFilterWriter(final TypeEncoder<K> convertorToBytes,
            final Hash newHash, final Directory directoryFacade,
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
        final int encodedLength = convertorToBytes.bytesLength(key);
        Vldtn.requireGreaterThanZero(encodedLength, "encodedLength");
        if (encodedKeyBuffer.length != encodedLength) {
            encodedKeyBuffer = new byte[encodedLength];
        }
        final int writtenBytes = convertorToBytes.toBytes(key, encodedKeyBuffer);
        if (writtenBytes != encodedLength) {
            throw new IllegalStateException(String.format(
                    "Encoder wrote '%s' bytes but declared '%s'", writtenBytes,
                    encodedLength));
        }
        return hash.store(encodedKeyBuffer);
    }

    @Override
    protected void doClose() {
        try (FileWriter writer = directoryFacade.getFileWriter(fileName,
                Access.OVERWRITE, diskIoBufferSize)) {
            writer.write(hash.getData());
        }
    }

}
