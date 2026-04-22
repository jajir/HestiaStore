package org.hestiastore.index.segmentindex.core.maintenance;

import java.util.concurrent.ExecutorService;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexConfiguration;

/**
 * Decorates executors with index-name logging context when enabled.
 */
final class IndexExecutorContextDecorator {

    private static final String ARG_EXECUTOR = "executor";
    private static final String ARG_INDEX_NAME = "indexName";

    private final IndexConfiguration<?, ?> configuration;

    IndexExecutorContextDecorator(
            final IndexConfiguration<?, ?> configuration) {
        this.configuration = Vldtn.requireNonNull(configuration,
                "configuration");
    }

    ExecutorService decorate(final ExecutorService executor) {
        final ExecutorService delegate = Vldtn.requireNonNull(executor,
                ARG_EXECUTOR);
        if (!Boolean.TRUE.equals(configuration.isContextLoggingEnabled())) {
            return delegate;
        }
        return new IndexNameMdcExecutorService(Vldtn.requireNotBlank(
                configuration.getIndexName(), ARG_INDEX_NAME), delegate);
    }
}
