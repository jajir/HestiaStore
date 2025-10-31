package org.hestiastore.index.directory;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.datatype.ByteSequenceAccumulator;
import org.hestiastore.index.Vldtn;

public class MemFileWriter extends AbstractCloseableResource
        implements FileWriter {

    private final String fileName;

    private final ByteSequenceAccumulator buffer;

    private final MemDirectory memDirectory;

    final Directory.Access access;

    MemFileWriter(final String fileName, final MemDirectory memDirectory,
            final Directory.Access access) {
        this.fileName = fileName;
        this.memDirectory = memDirectory;
        this.buffer = ByteSequenceAccumulator.create();
        this.access = access;
    }

    @Override
    protected void doClose() {
        final Bytes data = buffer.toBytes();
        buffer.close();
        memDirectory.addFile(fileName, data, access);
    }

    @Override
    public void write(byte b) {
        buffer.write(b);
    }

    @Override
    public void write(final ByteSequence bytes) {
        buffer.write(Vldtn.requireNonNull(bytes, "bytes"));
    }

}
