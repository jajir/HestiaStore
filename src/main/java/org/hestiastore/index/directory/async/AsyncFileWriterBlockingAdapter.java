package org.hestiastore.index.directory.async;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

/**
 * Blocking adapter that exposes an {@link AsyncFileWriter} as a synchronous
 * {@link FileWriter}.
 */
public final class AsyncFileWriterBlockingAdapter implements FileWriter {

    private final AsyncFileWriter delegate;

    public AsyncFileWriterBlockingAdapter(final AsyncFileWriter delegate) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
    }

    @Override
    public void write(final byte b) {
        delegate.writeAsync(b).toCompletableFuture().join();
    }

    @Override
    public void write(final byte[] bytes) {
        delegate.writeAsync(bytes).toCompletableFuture().join();
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
