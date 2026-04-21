package org.hestiastore.index.segmentindex.core.facade;

import java.util.NoSuchElementException;
import java.util.function.Supplier;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.slf4j.MDC;

/**
 * Entry iterator decorator that sets the {@code index.name} MDC key while
 * delegating iterator calls.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class EntryIteratorLoggingContext<K, V> extends AbstractCloseableResource
        implements EntryIterator<K, V> {

    private static final String INDEX_NAME_MDC_KEY = "index.name";

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

    @Override
    public boolean hasNext() {
        return runWithIndexName(entryIterator::hasNext);
    }

    @Override
    public Entry<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException(
                    "No more entries in the iterator.");
        }
        return runWithIndexName(entryIterator::next);
    }

    @Override
    protected void doClose() {
        runWithIndexName(() -> {
            entryIterator.close();
            return null;
        });
    }

    private <T> T runWithIndexName(final Supplier<T> action) {
        final String previousIndexName = MDC.get(INDEX_NAME_MDC_KEY);
        MDC.put(INDEX_NAME_MDC_KEY, indexName);
        try {
            return action.get();
        } finally {
            restoreIndexName(previousIndexName);
        }
    }

    private void restoreIndexName(final String previousIndexName) {
        if (previousIndexName == null) {
            MDC.remove(INDEX_NAME_MDC_KEY);
            return;
        }
        MDC.put(INDEX_NAME_MDC_KEY, previousIndexName);
    }
}
