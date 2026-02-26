package org.hestiastore.index.directory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;

/**
 * Filesystem seekable-reader supplier that keeps one shared channel open and
 * creates lightweight positioned cursors for each request.
 */
final class FsSharedFileReaderSeekableSupplier extends AbstractCloseableResource
        implements FileReaderSeekableSupplier {

    private final FileChannel channel;

    FsSharedFileReaderSeekableSupplier(final File file) {
        Vldtn.requireNonNull(file, "file");
        try {
            this.channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        } catch (final IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    @Override
    public FileReaderSeekable get() {
        if (wasClosed()) {
            throw new IllegalStateException(
                    getClass().getSimpleName() + " already closed");
        }
        return new PositionedCursor(channel);
    }

    @Override
    protected void doClose() {
        try {
            channel.close();
        } catch (final IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    /**
     * Per-request cursor that maintains independent position while sharing the
     * underlying channel.
     */
    private static final class PositionedCursor extends AbstractCloseableResource
            implements FileReaderSeekable {

        private final FileChannel channel;
        private final ByteBuffer singleByteBuffer = ByteBuffer.allocate(1);
        private long position;

        private PositionedCursor(final FileChannel channel) {
            this.channel = Vldtn.requireNonNull(channel, "channel");
            this.position = 0L;
        }

        @Override
        public int read() {
            try {
                singleByteBuffer.clear();
                final int read = channel.read(singleByteBuffer, position);
                if (read == -1) {
                    return -1;
                }
                position += read;
                singleByteBuffer.flip();
                return singleByteBuffer.get() & 0xFF;
            } catch (final IOException e) {
                throw new IndexException(e.getMessage(), e);
            }
        }

        @Override
        public int read(final byte[] bytes) {
            try {
                final ByteBuffer buffer = ByteBuffer.wrap(bytes);
                final int read = channel.read(buffer, position);
                if (read > 0) {
                    position += read;
                }
                return read;
            } catch (final IOException e) {
                throw new IndexException(e.getMessage(), e);
            }
        }

        @Override
        public void skip(final long bytesToSkip) {
            try {
                final long targetPosition = position + bytesToSkip;
                if (targetPosition > channel.size()) {
                    throw new IndexException(String.format(
                            "In file should be '%s' bytes skipped but "
                                    + "actually was skipped '%s' bytes.",
                            bytesToSkip, channel.size() - position));
                }
                this.position = targetPosition;
            } catch (final IOException e) {
                throw new IndexException(e.getMessage(), e);
            }
        }

        @Override
        public void seek(final long position) {
            this.position = position;
        }

        @Override
        protected void doClose() {
            // no-op: parent supplier owns the shared channel
        }
    }
}
