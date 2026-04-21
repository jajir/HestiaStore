package org.hestiastore.index.segmentindex.core.lifecycle;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.slf4j.MDC;

/**
 * Runs delegated operations with the index name present in MDC.
 */
final class IndexContextScopeRunner {

    private static final String INDEX_NAME_MDC_KEY = "index.name";
    private final String indexName;

    IndexContextScopeRunner(final String indexName) {
        this.indexName = Vldtn.requireNotBlank(indexName, "indexName");
    }

    void run(final Runnable action) {
        Vldtn.requireNonNull(action, "action");
        try (MdcScope ignored = openScope()) {
            action.run();
        }
    }

    <T> T supply(final Supplier<T> action) {
        Vldtn.requireNonNull(action, "action");
        try (MdcScope ignored = openScope()) {
            return action.get();
        }
    }

    private MdcScope openScope() {
        final String previousIndexName = MDC.get(INDEX_NAME_MDC_KEY);
        MDC.put(INDEX_NAME_MDC_KEY, indexName);
        return new MdcScope(previousIndexName);
    }

    private static final class MdcScope implements AutoCloseable {

        private final String previousIndexName;

        private MdcScope(final String previousIndexName) {
            this.previousIndexName = previousIndexName;
        }

        @Override
        public void close() {
            if (previousIndexName == null) {
                MDC.remove(INDEX_NAME_MDC_KEY);
                return;
            }
            MDC.put(INDEX_NAME_MDC_KEY, previousIndexName);
        }
    }
}
