package org.hestiastore.index.segmentindex;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.slf4j.MDC;

/**
 * Entry iterator decorator that sets the {@code index.name} MDC key while
 * delegating iteration calls.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class EntryIteratorLoggingContext<K, V> extends AbstractCloseableResource
        implements EntryIterator<K, V> {

    private final EntryIterator<K, V> entryIterator;
    private final IndexConfiguration<K, V> indexConf;

    EntryIteratorLoggingContext(final EntryIterator<K, V> entryIterator,
            final IndexConfiguration<K, V> indexConf) {
        this.entryIterator = Vldtn.requireNonNull(entryIterator, "entryIterator");
        this.indexConf = Vldtn.requireNonNull(indexConf, "indexConf");
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        setContext();
        try {
            return entryIterator.hasNext();
        } finally {
            clearContext();
        }
    }

    /** {@inheritDoc} */
    @Override
    public Entry<K, V> next() {
        setContext();
        try {
            return entryIterator.next();
        } finally {
            clearContext();
        }
    }

    @Override
    protected void doClose() {
        setContext();
        try {
            entryIterator.close();
        } finally {
            clearContext();
        }
    }

    private void setContext() {
        MDC.put("index.name", indexConf.getIndexName());
    }

    private void clearContext() {
        MDC.remove("index.name");
    }
}
