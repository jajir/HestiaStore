package org.hestiastore.index.directory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;

public class MemFileWriter extends AbstractCloseableResource
        implements FileWriter {

    private final String fileName;

    // FIXME replace with ByteArrayWriter
    private final ByteArrayOutputStream fio;

    private final MemDirectory memDirectory;

    final Directory.Access access;

    MemFileWriter(final String fileName, final MemDirectory memDirectory,
            final Directory.Access access) {
        this.fileName = fileName;
        this.memDirectory = memDirectory;
        this.fio = new ByteArrayOutputStream();
        this.access = access;
    }

    @Override
    protected void doClose() {
        try {
            fio.close();
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
        memDirectory.addFile(fileName, Bytes.of(fio.toByteArray()), access);
    }

    @Override
    public void write(byte b) {
        fio.write(b);
    }

    @Override
    public void write(final ByteSequence bytes) {
        final byte[] data = Vldtn.requireNonNull(bytes, "bytes").toByteArray();
        try {
            fio.write(data);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

}
