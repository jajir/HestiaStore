package org.hestiastore.index.segmentindex.core.routing;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexConfiguration;

/**
 * Applies optional cross-cutting decoration to iterators opened through the
 * data facade.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexEntryIteratorDecorator<K, V> {

    private final IndexConfiguration<K, V> conf;

    public SegmentIndexEntryIteratorDecorator(
            final IndexConfiguration<K, V> conf) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
    }

    public EntryIterator<K, V> decorate(final EntryIterator<K, V> iterator) {
        final EntryIterator<K, V> validatedIterator = Vldtn
                .requireNonNull(iterator, "iterator");
        if (!isContextLoggingEnabled()) {
            return validatedIterator;
        }
        return new EntryIteratorLoggingContext<>(validatedIterator, conf);
    }

    private boolean isContextLoggingEnabled() {
        final Boolean enabled = conf.isContextLoggingEnabled();
        return enabled != null && enabled;
    }
}
