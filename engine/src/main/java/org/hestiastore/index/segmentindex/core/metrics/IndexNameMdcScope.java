package org.hestiastore.index.segmentindex.core.metrics;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.slf4j.MDC;

/**
 * Restores the previous {@code index.name} MDC value when closed.
 */
final class IndexNameMdcScope implements AutoCloseable {

    private static final String INDEX_NAME_MDC_KEY = "index.name";
    private static final IndexNameMdcScope NOOP = new IndexNameMdcScope(null,
            false);

    private final String previousIndexName;
    private final boolean active;

    private IndexNameMdcScope(final String previousIndexName,
            final boolean active) {
        this.previousIndexName = previousIndexName;
        this.active = active;
    }

    static IndexNameMdcScope open(final String indexName) {
        final String previousIndexName = MDC.get(INDEX_NAME_MDC_KEY);
        MDC.put(INDEX_NAME_MDC_KEY,
                Vldtn.requireNotBlank(indexName, "indexName"));
        return new IndexNameMdcScope(previousIndexName, true);
    }

    static IndexNameMdcScope openIfConfigured(
            final IndexConfiguration<?, ?> conf) {
        final IndexConfiguration<?, ?> configuration = Vldtn.requireNonNull(conf,
                "conf");
        if (!Boolean.TRUE.equals(configuration.isContextLoggingEnabled())) {
            return NOOP;
        }
        final String normalizedIndexName = normalizeIndexName(
                configuration.getIndexName());
        if (normalizedIndexName == null) {
            return NOOP;
        }
        return open(normalizedIndexName);
    }

    @Override
    public void close() {
        if (!active) {
            return;
        }
        restorePreviousIndexName(previousIndexName);
    }

    static void restorePreviousIndexName(
            final String previousIndexName) {
        if (previousIndexName == null) {
            MDC.remove(INDEX_NAME_MDC_KEY);
            return;
        }
        MDC.put(INDEX_NAME_MDC_KEY, previousIndexName);
    }

    private static String normalizeIndexName(final String indexName) {
        if (indexName == null) {
            return null;
        }
        final String normalized = indexName.trim();
        return normalized.isEmpty() ? null : normalized;
    }
}
