package org.hestiastore.index.directory.async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReader;

/**
 * Default {@link AsyncFileReader} that delegates blocking operations to a
 * supplied executor.
 */
class AsyncFileReaderAdapter extends AbstractCloseableResource
        implements AsyncFileReader {

    private final FileReader delegate;
    private final Executor executor;
    AsyncFileReaderAdapter(final FileReader delegate, final Executor executor) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.executor = Vldtn.requireNonNull(executor, "executor");
    }

    @Override
    public CompletionStage<Integer> readAsync() {
        return supply(() -> delegate.read());
    }

    @Override
    public CompletionStage<Integer> readAsync(final byte[] bytes) {
        return supply(() -> delegate.read(bytes));
    }

    @Override
    public CompletionStage<Void> skipAsync(final long position) {
        return supply(() -> {
            delegate.skip(position);
            return null;
        });
    }

    @Override
    protected void doClose() {
        delegate.close();
    }

    protected <T> CompletionStage<T> supply(final Supplier<T> supplier) {
        if (wasClosed()) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "AsyncFileReader already closed"));
        }
        return CompletableFuture.supplyAsync(() -> {
            if (wasClosed()) {
                throw new IllegalStateException(
                        "AsyncFileReader already closed");
            }
            return supplier.get();
        }, executor);
    }
}
