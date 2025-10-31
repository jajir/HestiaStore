package org.hestiastore.index.directory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.hestiastore.index.AbstractCloseableResource;
import java.util.Arrays;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.MutableByteSequence;
import org.hestiastore.index.MutableBytes;
import org.hestiastore.index.Vldtn;

public final class FsFileReaderStream extends AbstractCloseableResource
        implements FileReader {

    private final BufferedInputStream bis;

    FsFileReaderStream(final File file, final int bufferSize) {
        try {
            final Path path = file.toPath();
            final InputStream fin = Files.newInputStream(path,
                    StandardOpenOption.READ);
            bis = new BufferedInputStream(fin, bufferSize);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    protected void doClose() {
        try {
            bis.close();
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public int read() {
        try {
            return bis.read();
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
            final int readBytes = bis.read(data, 0, length);
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
            long skippedBytes = bis.skip(bytesToSkip);
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
    public String toString() {
        return String.format("FsFileReaderStream[bis='%s']", bis.toString());
    }

}
