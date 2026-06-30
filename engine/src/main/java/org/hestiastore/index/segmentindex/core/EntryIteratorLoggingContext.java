package org.hestiastore.index.segmentindex.core;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexConfiguration;

/**
 * Entry iterator decorator that sets the {@code index.name} MDC key while
 * delegating iteration calls.
 *
 * @param <K> key type
 * @param <V> value type
 */
class EntryIteratorLoggingContext<K, V> extends AbstractCloseableResource
        implements EntryIterator<K, V> {

    private final EntryIterator<K, V> entryIterator;
    private final String indexName;

    EntryIteratorLoggingContext(final EntryIterator<K, V> entryIterator,
            final IndexConfiguration<K, V> indexConf) {
        this.entryIterator = Vldtn.requireNonNull(entryIterator, "entryIterator");
        final IndexConfiguration<K, V> configuration = Vldtn
                .requireNonNull(indexConf, "indexConf");
        this.indexName = Vldtn.requireNotBlank(configuration.getIndexName(),
                "indexName");
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        try (IndexNameMdcScope ignored = IndexNameMdcScope.open(indexName)) {
            return entryIterator.hasNext();
        }
    }

    /** {@inheritDoc} */
    @Override
    public Entry<K, V> next() {
        try (IndexNameMdcScope ignored = IndexNameMdcScope.open(indexName)) {
            return entryIterator.next();
        }
    }

    @Override
    protected void doClose() {
        try (IndexNameMdcScope ignored = IndexNameMdcScope.open(indexName)) {
            entryIterator.close();
        }
    }
}
