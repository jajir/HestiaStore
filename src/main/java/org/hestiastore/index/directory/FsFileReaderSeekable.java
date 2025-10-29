package org.hestiastore.index.directory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.MutableByteSequence;
import org.hestiastore.index.Vldtn;

public final class FsFileReaderSeekable extends AbstractCloseableResource
        implements FileReaderSeekable {

    private final RandomAccessFile raf;

    FsFileReaderSeekable(final File file) {
        try {
            raf = new RandomAccessFile(file, "r");
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public int read() {
        try {
            return raf.read();
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public int read(final MutableByteSequence bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        final byte[] data = new byte[bytes.length()];
        try {
            final int read = raf.read(data, 0, data.length);
            if (read > 0) {
                bytes.setBytes(0, MutableByteSequence.wrap(data), 0, read);
            }
            return read;
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public void skip(final long bytesToSkip) {
        try {
            int skippedBytes = raf.skipBytes((int) bytesToSkip);
            if (skippedBytes != bytesToSkip) {
                throw new IndexException(String.format(
                        "In file should be '%s' bytes skipped but "
                                + "actually was skipped '%s' bytes.",
                        bytesToSkip, skippedBytes));
            }
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    protected void doClose() {
        try {
            raf.close();
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public void seek(final long position) {
        try {
            raf.seek(position);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

}
