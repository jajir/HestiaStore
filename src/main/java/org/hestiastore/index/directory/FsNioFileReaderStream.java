package org.hestiastore.index.directory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.MutableByteSequence;
import org.hestiastore.index.Vldtn;

/**
 * Same as FsFileReaderStream but uses java.nio.
 */
public final class FsNioFileReaderStream extends AbstractCloseableResource
        implements FileReader {

    private final FileChannel channel;

    public FsNioFileReaderStream(final File file) {
        try {
            channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public int read() {
        ByteBuffer oneByte = ByteBuffer.allocate(1);
        try {
            int readBytes = channel.read(oneByte);
            if (readBytes == -1) {
                return -1;
            }
            oneByte.flip();
            return oneByte.get() & 0xFF;
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public int read(final MutableByteSequence bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        final byte[] data = new byte[bytes.length()];
        final ByteBuffer buffer = ByteBuffer.wrap(data, 0, data.length);
        try {
            final int read = channel.read(buffer);
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
            long currentPos = channel.position();
            channel.position(currentPos + bytesToSkip);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    protected void doClose() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        return String.format("FsNioFileReaderStream[channel='%s']",
                channel.toString());
    }
}
