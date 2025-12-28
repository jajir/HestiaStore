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
