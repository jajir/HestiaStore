package org.hestiastore.index.directory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.IndexException;

/**
 * {@link FileReaderSeekable} implementation backed by a {@link FileChannel}.
 * The reader exposes byte-wise and bulk read operations, supports seeking /
 * skipping, and closes the channel when the resource is released.
 */
public final class FsFileReaderSeekable extends AbstractCloseableResource
        implements FileReaderSeekable {

    private final FileChannel channel;
    private final ByteBuffer singleByteBuffer = ByteBuffer.allocate(1);

    /**
     * Opens the given file for read-only, seekable access.
     *
     * @param file file to read from
     * @throws IndexException when the file channel cannot be opened
     */
    FsFileReaderSeekable(final File file) {
        try {
            channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    /**
     * Reads a single unsigned byte from the current channel position.
     *
     * @return byte value (0-255) or {@code -1} when end of stream is reached
     * @throws IndexException when the underlying channel read fails
     */
    @Override
    public int read() {
        try {
            singleByteBuffer.clear();
            final int read = channel.read(singleByteBuffer);
            if (read == -1) {
                return -1;
            }
            singleByteBuffer.flip();
            return singleByteBuffer.get() & 0xFF;
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    /**
     * Attempts to read enough bytes to fill the supplied array or until EOF.
     *
     * @param bytes destination buffer
     * @return number of bytes read or {@code -1} when the end of stream is hit
     * @throws IndexException when the underlying channel read fails
     */
    @Override
    public int read(final byte[] bytes) {
        try {
            final ByteBuffer buffer = ByteBuffer.wrap(bytes);
            return channel.read(buffer);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    /**
     * Advances the position by the specified number of bytes. The method
     * verifies the skip does not go beyond the end of the file.
     *
     * @param bytesToSkip number of bytes to move forward
     * @throws IndexException when the skip would exceed file size or on I/O
     */
    @Override
    public void skip(final long bytesToSkip) {
        try {
            final long currentPosition = channel.position();
            final long targetPosition = currentPosition + bytesToSkip;
            if (targetPosition > channel.size()) {
                throw new IndexException(String.format(
                        "In file should be '%s' bytes skipped but "
                                + "actually was skipped '%s' bytes.",
                        bytesToSkip, channel.size() - currentPosition));
            }
            channel.position(targetPosition);
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

    /**
     * Moves the current position to the given absolute offset.
     *
     * @param position zero-based offset inside the file
     * @throws IndexException when positioning fails
     */
    @Override
    public void seek(final long position) {
        try {
            channel.position(position);
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

}
