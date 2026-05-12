package org.hestiastore.index.segmentindex.core;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;

/**
 * Runs delegated operations with the index name present in MDC.
 */
public final class IndexMdcScopeRunner {

    private final String indexName;

    public IndexMdcScopeRunner(final String indexName) {
        this.indexName = Vldtn.requireNotBlank(indexName, "indexName");
    }

    public IndexMdcScope openScope() {
        return new IndexMdcScope(indexName);
    }

    public void run(final Runnable action) {
        Vldtn.requireNonNull(action, "action");
        try (IndexMdcScope ignored = openScope()) {
            action.run();
        }
    }

    public <T> T supply(final Supplier<T> supplier) {
        Vldtn.requireNonNull(supplier, "supplier");
        try (IndexMdcScope ignored = openScope()) {
            return supplier.get();
        }
    }

}
