package org.hestiastore.index.directory.async;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReaderSeekable;

/**
 * Blocking adapter that exposes an {@link AsyncFileReaderSeekable} as a
 * synchronous {@link FileReaderSeekable}.
 */
public final class AsyncFileReaderSeekableBlockingAdapter
        implements FileReaderSeekable {

    private final AsyncFileReaderSeekable delegate;

    public AsyncFileReaderSeekableBlockingAdapter(
            final AsyncFileReaderSeekable delegate) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
    }

    @Override
    public int read() {
        return delegate.readAsync().toCompletableFuture().join();
    }

    @Override
    public int read(final byte[] bytes) {
        return delegate.readAsync(bytes).toCompletableFuture().join();
    }

    @Override
    public int read(final byte[] bytes, final int offset, final int length) {
        Vldtn.requireNonNull(bytes, "bytes");
        if (offset < 0 || length < 0 || offset > bytes.length
                || offset + length > bytes.length) {
            throw new IllegalArgumentException(String.format(
                    "Range [%d, %d) exceeds array length %d", offset,
                    offset + length, bytes.length));
        }
        if (length == 0) {
            return 0;
        }
        return delegate.readAsync(bytes, offset, length).toCompletableFuture()
                .join();
    }

    @Override
    public void skip(final long position) {
        delegate.skipAsync(position).toCompletableFuture().join();
    }

    @Override
    public void seek(final long position) {
        delegate.seekAsync(position).toCompletableFuture().join();
    }

    @Override
    public boolean wasClosed() {
        return delegate.wasClosed();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
