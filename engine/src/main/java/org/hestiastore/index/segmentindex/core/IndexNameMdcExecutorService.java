package org.hestiastore.index.segmentindex.core;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.Vldtn;

/**
 * Executor service decorator that enforces {@code index.name} MDC for every
 * executed task.
 */
final class IndexNameMdcExecutorService extends AbstractExecutorService {

    private final String indexName;
    private final ExecutorService delegate;

    IndexNameMdcExecutorService(final String indexName,
            final ExecutorService delegate) {
        this.indexName = Vldtn.requireNotBlank(indexName, "indexName");
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit)
            throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public void execute(final Runnable command) {
        delegate.execute(withIndexContext(command));
    }

    private Runnable withIndexContext(final Runnable command) {
        final Runnable task = Vldtn.requireNonNull(command, "command");
        return () -> {
            try (IndexNameMdcScope ignored = IndexNameMdcScope
                    .open(indexName)) {
                task.run();
            }
        };
    }
}
