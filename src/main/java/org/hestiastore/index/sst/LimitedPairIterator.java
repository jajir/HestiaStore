package org.hestiastore.index.sst;

import java.util.Comparator;
import java.util.NoSuchElementException;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.Vldtn;

public final class LimitedPairIterator<K, V> implements PairIterator<K, V> {

    private final PairIterator<K, V> iterator;
    private final Comparator<K> keyComparator;
    private final K minKey;
    private final K maxKey;

    private Pair<K, V> nextPair = null;

    LimitedPairIterator(final PairIterator<K, V> iterator,
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
        nextPair = iterator.next();
        while (!isInRange(nextPair) && iterator.hasNext()) {
            nextPair = iterator.next();
        }
        if (!iterator.hasNext()) {
            nextPair = null;
        }
    }

    @Override
    public boolean hasNext() {
        return nextPair != null;
    }

    @Override
    public Pair<K, V> next() {
        final Pair<K, V> out = nextPair;
        if (nextPair == null) {
            throw new NoSuchElementException("There no next element.");
        } else {
            if (iterator.hasNext()) {
                nextPair = iterator.next();
                if (!isInRange(nextPair)) {
                    nextPair = null;
                }
            } else {
                nextPair = null;
            }
        }
        return out;
    }

    @Override
    public void close() {
        iterator.close();
    }

    private boolean isInRange(final Pair<K, V> pair) {
        if (pair == null) {
            return false;
        }

        return keyComparator.compare(pair.getKey(), minKey) >= 0
                && keyComparator.compare(pair.getKey(), maxKey) <= 0;
    }
}
