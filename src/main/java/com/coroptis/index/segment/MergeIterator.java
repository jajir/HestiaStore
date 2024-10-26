package com.coroptis.index.segment;

import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.datatype.TypeDescriptor;

/**
 * Allows to create final stream of data from cache and SST. Tombstones are
 * applied.
 * 
 * FIXME: Remove this class
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class MergeIterator<K, V> implements PairIterator<K, V> {

    /**
     * Iterator contains main data with deleted items. Can't contains
     * tombstones.
     */
    private final PairIterator<K, V> mainIterator;

    /**
     * Cached data iterator. Can contains tombstones.
     */
    private final PairIterator<K, V> cacheIterator;

    private final TypeDescriptor<V> valueTypeDescriptor;

    private final Comparator<K> keyComparator;

    private Pair<K, V> currentPair = null;
    private Pair<K, V> nextMainPair = null;
    private Pair<K, V> nextCachePair = null;

    public MergeIterator(final PairIterator<K, V> mainIterator,
            final PairIterator<K, V> cacheIterator,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.mainIterator = Objects.requireNonNull(mainIterator);
        this.cacheIterator = Objects.requireNonNull(cacheIterator);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        Objects.requireNonNull(keyTypeDescriptor);
        this.keyComparator = keyTypeDescriptor.getComparator();

        nextMainIterator();
        nextCacheIterator();
        tryToremoveTombstone();
    }

    @Override
    public boolean hasNext() {
        return nextCachePair != null || nextMainPair != null;
    }

    @Override
    public Pair<K, V> next() {
        if (nextMainPair == null) {
            if (nextCachePair == null) {
                throw new NoSuchElementException("There no next element.");
            } else {
                currentPair = nextCacheIterator();
                tryToremoveTombstone();
            }
        } else {
            if (nextCachePair == null) {
                currentPair = nextMainIterator();
                tryToremoveTombstone();
            } else {
                // both next elements exists
                final int cmp = keyComparator.compare(nextMainPair.getKey(),
                        nextCachePair.getKey());
                if (cmp < 0) {
                    currentPair = nextMainIterator();
                    tryToremoveTombstone();
                } else if (cmp == 0) {
                    if (valueTypeDescriptor
                            .isTombstone(nextCachePair.getValue())) {
                        nextMainIterator();
                        nextCacheIterator();
                        tryToremoveTombstone();
                        next();
                    } else {
                        currentPair = nextCachePair;
                    }
                    nextMainIterator();
                    nextCacheIterator();
                    tryToremoveTombstone();
                } else {
                    currentPair = nextCacheIterator();
                    tryToremoveTombstone();
                }
            }
        }
        return currentPair;
    }

    private Pair<K, V> nextMainIterator() {
        final Pair<K, V> outPair = nextMainPair;
        if (mainIterator.hasNext()) {
            nextMainPair = mainIterator.next();
        } else {
            nextMainPair = null;
        }
        return outPair;
    }

    private Pair<K, V> nextCacheIterator() {
        final Pair<K, V> outPair = nextCachePair;
        if (cacheIterator.hasNext()) {
            nextCachePair = cacheIterator.next();
        } else {
            nextCachePair = null;
        }
        return outPair;
    }

    private void tryToremoveTombstone() {
        if (nextCachePair == null) {
            return;
        }
        if (nextMainPair == null) {
            if (valueTypeDescriptor.isTombstone(nextCachePair.getValue())) {
                nextCacheIterator();
                tryToremoveTombstone();
            }
            return;
        }
        if (valueTypeDescriptor.isTombstone(nextCachePair.getValue())) {
            final int cmp = keyComparator.compare(nextMainPair.getKey(),
                    nextCachePair.getKey());
            if (cmp == 0) {
                nextMainIterator();
                nextCacheIterator();
                tryToremoveTombstone();
            } else if (cmp > 0) {
                nextCacheIterator();
            }
        }
    }

    @Override
    public void close() {
        mainIterator.close();
        cacheIterator.close();
    }

}
