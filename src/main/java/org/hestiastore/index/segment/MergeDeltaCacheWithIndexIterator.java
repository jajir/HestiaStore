package org.hestiastore.index.segment;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;

public class MergeDeltaCacheWithIndexIterator<K, V>
        implements PairIterator<K, V> {

    private final PairIterator<K, V> mainIterator;
    private final Iterator<Pair<K, V>> deltaCacheIterator;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final Comparator<K> keyComparator;

    private Pair<K, V> next = null;

    private Pair<K, V> mainCurrent;
    private Pair<K, V> deltaCurrent;

    public MergeDeltaCacheWithIndexIterator(
            final PairIterator<K, V> mainIterator,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final List<Pair<K, V>> sortedDeltaCache) {

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
    public void close() {
        mainIterator.close();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public Pair<K, V> next() {
        if (next == null) {
            throw new NoSuchElementException("No next element.");
        }
        Pair<K, V> result = next;
        advance();
        return result;
    }

    private void advance() {
        next = null;
        // Is there aany data to examine?
        while (mainCurrent != null || deltaCurrent != null) {
            Pair<K, V> candidate;
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

    private Pair<K, V> readNextPairFromMain() {
        if (mainIterator.hasNext()) {
            return mainIterator.next();
        } else {
            return null;
        }
    }

    private Pair<K, V> readNextPairFromCache() {
        if (deltaCacheIterator.hasNext()) {
            return deltaCacheIterator.next();
        } else {
            return null;
        }
    }
}