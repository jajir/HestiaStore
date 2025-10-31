package org.hestiastore.index.directory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.MutableByteSequence;
import org.hestiastore.index.MutableBytes;
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
        final MutableByteSequence buffer = Vldtn.requireNonNull(bytes, "bytes");
        final int length = buffer.length();
        final MutableBytes directBuffer = buffer instanceof MutableBytes
                ? (MutableBytes) buffer
                : null;
        final byte[] data = directBuffer != null ? directBuffer.array()
                : new byte[length];
        try {
            final int readBytes = raf.read(data, 0, length);
            if (directBuffer == null && readBytes > 0) {
                buffer.setBytes(0,
                        MutableBytes.wrap(readBytes == data.length ? data
                                : Arrays.copyOf(data, readBytes)),
                        0, readBytes);
            }
            return readBytes;
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
