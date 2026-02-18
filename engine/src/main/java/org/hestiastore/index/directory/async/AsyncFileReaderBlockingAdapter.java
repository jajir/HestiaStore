package org.hestiastore.index.directory.async;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReader;

/**
 * Blocking adapter that exposes an {@link AsyncFileReader} as a synchronous
 * {@link FileReader}.
 */
public final class AsyncFileReaderBlockingAdapter implements FileReader {

    private final AsyncFileReader delegate;

    public AsyncFileReaderBlockingAdapter(final AsyncFileReader delegate) {
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
    public boolean wasClosed() {
        return delegate.wasClosed();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
