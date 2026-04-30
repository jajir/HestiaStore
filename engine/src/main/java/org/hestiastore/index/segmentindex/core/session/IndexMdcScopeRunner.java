package org.hestiastore.index.segmentindex.core.session;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.slf4j.MDC;

/**
 * Runs delegated operations with the index name present in MDC.
 */
public final class IndexMdcScopeRunner {

    private static final String INDEX_NAME_MDC_KEY = "index.name";
    private final String indexName;

    public IndexMdcScopeRunner(final String indexName) {
        this.indexName = Vldtn.requireNotBlank(indexName, "indexName");
    }

    public void run(final Runnable action) {
        Vldtn.requireNonNull(action, "action");
        final String previousIndexName = MDC.get(INDEX_NAME_MDC_KEY);
        try {
            MDC.put(INDEX_NAME_MDC_KEY, indexName);
            action.run();
        } finally {
            if (previousIndexName == null) {
                MDC.remove(INDEX_NAME_MDC_KEY);
            } else {
                MDC.put(INDEX_NAME_MDC_KEY, previousIndexName);
            }
        }
    }

    public <T> T supply(final Supplier<T> supplier) {
        Vldtn.requireNonNull(supplier, "supplier");
        final String previousIndexName = MDC.get(INDEX_NAME_MDC_KEY);
        try {
            MDC.put(INDEX_NAME_MDC_KEY, indexName);
            return supplier.get();
        } finally {
            if (previousIndexName == null) {
                MDC.remove(INDEX_NAME_MDC_KEY);
            } else {
                MDC.put(INDEX_NAME_MDC_KEY, previousIndexName);
            }
        }
    }

}
