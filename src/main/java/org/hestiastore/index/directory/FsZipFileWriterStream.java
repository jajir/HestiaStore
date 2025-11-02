package org.hestiastore.index.directory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;

public final class FsZipFileWriterStream extends AbstractCloseableResource
        implements FileWriter {

    private final ZipOutputStream fio;

    private static final int BUFFER_SIZE = 1024 * 100;

    FsZipFileWriterStream(final File file) {
        try {
            this.fio = new ZipOutputStream(new BufferedOutputStream(
                    new FileOutputStream(file), BUFFER_SIZE));
            fio.setMethod(ZipOutputStream.DEFLATED);
            fio.setLevel(9);
            fio.putNextEntry(new ZipEntry("default.dat"));
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    protected void doClose() {
        try {
            fio.closeEntry();
            fio.close();
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public void write(byte b) {
        try {
            fio.write(b);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
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
