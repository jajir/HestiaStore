package org.hestiastore.index.segment;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;

public class MergeDeltaCacheWithIndexIterator<K, V>
        extends AbstractCloseableResource implements EntryIterator<K, V> {

    private final EntryIterator<K, V> mainIterator;
    private final Iterator<Entry<K, V>> deltaCacheIterator;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final Comparator<K> keyComparator;

    private Entry<K, V> next = null;

    private Entry<K, V> mainCurrent;
    private Entry<K, V> deltaCurrent;

    public MergeDeltaCacheWithIndexIterator(
            final EntryIterator<K, V> mainIterator,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final List<Entry<K, V>> sortedDeltaCache) {

        this.mainIterator = Vldtn.requireNonNull(mainIterator, "mainIterator");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.keyComparator = Vldtn
                .requireNonNull(keyTypeDescriptor, "keyTypeDescriptor")
                .getComparator();
        Vldtn.requireNonNull(sortedDeltaCache, "sortedDeltaCache");

        this.deltaCacheIterator = sortedDeltaCache.iterator();
        this.mainCurrent = readNextPairFromMain();
        this.deltaCurrent = readNextPairFromCache();

        advance();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public Entry<K, V> next() {
        if (next == null) {
            throw new NoSuchElementException("No next element.");
        }
        Entry<K, V> result = next;
        advance();
        return result;
    }

    private void advance() {
        next = null;
        // Is there aany data to examine?
        while (mainCurrent != null || deltaCurrent != null) {
            Entry<K, V> candidate;
            if (mainCurrent == null) {
                candidate = deltaCurrent;
                deltaCurrent = readNextPairFromCache();
            } else if (deltaCurrent == null) {
                candidate = mainCurrent;
                mainCurrent = readNextPairFromMain();
            } else {
                int cmp = keyComparator.compare(mainCurrent.getKey(),
                        deltaCurrent.getKey());
                if (cmp < 0) {
                    candidate = mainCurrent;
                    mainCurrent = readNextPairFromMain();
                } else if (cmp > 0) {
                    candidate = deltaCurrent;
                    deltaCurrent = readNextPairFromCache();
                } else {
                    // same key: use delta version, skip both
                    candidate = deltaCurrent;
                    mainCurrent = readNextPairFromMain();
                    deltaCurrent = readNextPairFromCache();
                }
            }

            if (!valueTypeDescriptor.isTombstone(candidate.getValue())) {
                next = candidate;
                return;
            }
            // else skip tombstone and continue
        }
    }

    private Entry<K, V> readNextPairFromMain() {
        if (mainIterator.hasNext()) {
            return mainIterator.next();
        } else {
            return null;
        }
    }

    private Entry<K, V> readNextPairFromCache() {
        if (deltaCacheIterator.hasNext()) {
            return deltaCacheIterator.next();
        } else {
            return null;
        }
    }

    @Override
    protected void doClose() {
        mainIterator.close();
    }
}
