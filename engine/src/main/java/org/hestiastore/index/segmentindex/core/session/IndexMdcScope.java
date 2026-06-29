package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.slf4j.MDC;

/**
 * Holds one active index-name MDC scope.
 */
final class IndexMdcScope implements AutoCloseable {

    private static final String INDEX_NAME_MDC_KEY = "index.name";

    private final String previousIndexName;
    private boolean closed;

    IndexMdcScope(final String indexName) {
        previousIndexName = MDC.get(INDEX_NAME_MDC_KEY);
        MDC.put(INDEX_NAME_MDC_KEY,
                Vldtn.requireNotBlank(indexName, "indexName"));
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (previousIndexName == null) {
            MDC.remove(INDEX_NAME_MDC_KEY);
        } else {
            MDC.put(INDEX_NAME_MDC_KEY, previousIndexName);
        }
    }
}
