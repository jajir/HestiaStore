package org.hestiastore.index.segmentindex;

import java.util.Comparator;
import java.util.NoSuchElementException;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;

public final class LimitedEntryIterator<K, V> extends AbstractCloseableResource
        implements EntryIterator<K, V> {

    private final EntryIterator<K, V> iterator;
    private final Comparator<K> keyComparator;
    private final K minKey;
    private final K maxKey;

    private Entry<K, V> nextEntry = null;

    LimitedEntryIterator(final EntryIterator<K, V> iterator,
            final Comparator<K> keyComparator, final K minKey, final K maxKey) {
        this.iterator = Vldtn.requireNonNull(iterator, "iterator");
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        this.minKey = Vldtn.requireNonNull(minKey, "minKey");
        this.maxKey = Vldtn.requireNonNull(maxKey, "");
        if (keyComparator.compare(minKey, maxKey) > 0) {
            throw new IllegalArgumentException(String.format(
                    "Min key '%s' have to be smalles than max key '%s'.",
                    minKey, maxKey));
        }

        if (!iterator.hasNext()) {
            return;
        }
        nextEntry = iterator.next();
        while (!isInRange(nextEntry) && iterator.hasNext()) {
            nextEntry = iterator.next();
        }
        if (!iterator.hasNext()) {
            nextEntry = null;
        }
    }

    @Override
    public boolean hasNext() {
        return nextEntry != null;
    }

    @Override
    public Entry<K, V> next() {
        final Entry<K, V> out = nextEntry;
        if (nextEntry == null) {
            throw new NoSuchElementException("There no next element.");
        } else {
            if (iterator.hasNext()) {
                nextEntry = iterator.next();
                if (!isInRange(nextEntry)) {
                    nextEntry = null;
                }
            } else {
                nextEntry = null;
            }
        }
        return out;
    }

    @Override
    protected void doClose() {
        iterator.close();
    }

    private boolean isInRange(final Entry<K, V> entry) {
        if (entry == null) {
            return false;
        }

        return keyComparator.compare(entry.getKey(), minKey) >= 0
                && keyComparator.compare(entry.getKey(), maxKey) <= 0;
    }
}
