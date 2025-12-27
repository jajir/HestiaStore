package org.hestiastore.index.directory.async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.concurrent.locks.ReentrantLock;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

/**
 * Default {@link AsyncFileWriter} that delegates blocking writes to a supplied
 * executor.
 */
class AsyncFileWriterAdapter extends AbstractCloseableResource
        implements AsyncFileWriter {

    private final FileWriter delegate;
    private final Executor executor;
    private final ReentrantLock lock = new ReentrantLock();

    AsyncFileWriterAdapter(final FileWriter delegate, final Executor executor) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.executor = Vldtn.requireNonNull(executor, "executor");
    }

    @Override
    public CompletionStage<Void> writeAsync(final byte b) {
        return run(() -> delegate.write(b));
    }

    @Override
    public CompletionStage<Void> writeAsync(final byte[] bytes) {
        return run(() -> delegate.write(bytes));
    }

    @Override
    protected void doClose() {
        lock.lock();
        try {
            delegate.close();
        } finally {
            lock.unlock();
        }
    }

    private CompletionStage<Void> run(final Runnable runnable) {
        return supply(() -> {
            runnable.run();
            return null;
        });
    }

    private <T> CompletionStage<T> supply(final Supplier<T> supplier) {
        if (wasClosed()) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "AsyncFileWriter already closed"));
        }
        return CompletableFuture.supplyAsync(() -> {
            lock.lock();
            try {
                if (wasClosed()) {
                    throw new IllegalStateException(
                            "AsyncFileWriter already closed");
                }
                return supplier.get();
            } finally {
                lock.unlock();
            }
        }, executor);
    }
}

