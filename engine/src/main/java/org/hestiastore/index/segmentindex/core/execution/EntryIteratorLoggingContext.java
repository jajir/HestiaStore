package org.hestiastore.index.segmentindex.core.execution;

import java.util.NoSuchElementException;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.slf4j.MDC;

/**
 * Entry iterator decorator that sets the {@code index.name} MDC key while
 * delegating iterator calls.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class EntryIteratorLoggingContext<K, V> extends AbstractCloseableResource
        implements EntryIterator<K, V> {

    private static final String INDEX_NAME_MDC_KEY = "index.name";

    private final EntryIterator<K, V> entryIterator;
    private final String indexName;

    /**
     * Creates a context-logging iterator wrapper.
     *
     * @param entryIterator wrapped iterator
     * @param indexConf index configuration supplying the MDC index name
     */
    public EntryIteratorLoggingContext(final EntryIterator<K, V> entryIterator,
            final EffectiveIndexConfiguration<K, V> indexConf) {
        this.entryIterator = Vldtn.requireNonNull(entryIterator, "entryIterator");
        final EffectiveIndexConfiguration<K, V> configuration = Vldtn
                .requireNonNull(indexConf, "indexConf");
        this.indexName = Vldtn.requireNotBlank(configuration.identity().name(),
                "indexName");
    }

    @Override
    public boolean hasNext() {
        final String previousIndexName = openIndexName();
        try {
            return entryIterator.hasNext();
        } finally {
            restoreIndexName(previousIndexName);
        }
    }

    @Override
    public Entry<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException(
                    "No more entries in the iterator.");
        }
        final String previousIndexName = openIndexName();
        try {
            return entryIterator.next();
        } finally {
            restoreIndexName(previousIndexName);
        }
    }

    @Override
    protected void doClose() {
        final String previousIndexName = openIndexName();
        try {
            entryIterator.close();
        } finally {
            restoreIndexName(previousIndexName);
        }
    }

    private String openIndexName() {
        final String previousIndexName = MDC.get(INDEX_NAME_MDC_KEY);
        MDC.put(INDEX_NAME_MDC_KEY, indexName);
        return previousIndexName;
    }

    private void restoreIndexName(final String previousIndexName) {
        if (previousIndexName == null) {
            MDC.remove(INDEX_NAME_MDC_KEY);
            return;
        }
        MDC.put(INDEX_NAME_MDC_KEY, previousIndexName);
    }
}
