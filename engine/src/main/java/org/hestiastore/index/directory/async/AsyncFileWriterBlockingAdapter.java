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
    public void write(final byte[] bytes, final int offset, final int length) {
        final byte[] validated = Vldtn.requireNonNull(bytes, "bytes");
        final int from = Vldtn.requireGreaterThanOrEqualToZero(offset,
                "offset");
        final int len = Vldtn.requireGreaterThanOrEqualToZero(length,
                "length");
        if (from > validated.length || from + len > validated.length) {
            throw new IllegalArgumentException(String.format(
                    "Offset '%s' and length '%s' exceed source length '%s'",
                    from, len, validated.length));
        }
        if (len == 0) {
            return;
        }
        delegate.writeAsync(validated, from, len).toCompletableFuture().join();
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
