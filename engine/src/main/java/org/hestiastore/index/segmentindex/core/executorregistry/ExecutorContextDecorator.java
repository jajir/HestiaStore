package org.hestiastore.index.segmentindex.core.executorregistry;

import java.util.concurrent.ExecutorService;

import org.hestiastore.index.Vldtn;

/**
 * Decorates executors with index-name logging context when enabled.
 */
final class ExecutorContextDecorator {

    private static final String ARG_EXECUTOR = "executor";

    private final boolean contextLoggingEnabled;
    private final String indexName;

    ExecutorContextDecorator(final boolean contextLoggingEnabled,
            final String indexName) {
        this.contextLoggingEnabled = contextLoggingEnabled;
        this.indexName = indexName;
    }

    ExecutorService decorate(final ExecutorService executor) {
        final ExecutorService delegate = Vldtn.requireNonNull(executor,
                ARG_EXECUTOR);
        if (!contextLoggingEnabled) {
            return delegate;
        }
        return new IndexNameMdcExecutorService(indexName, delegate);
    }
}
