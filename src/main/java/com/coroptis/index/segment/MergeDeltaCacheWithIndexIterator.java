package com.coroptis.index.segment;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.datatype.TypeDescriptor;

/**
 * This iterator merge non modifiable data from file with cached data. Cached
 * value is obtained from cache just before returning. It make sure that data
 * are actual.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class MergeDeltaCacheWithIndexIterator<K, V> implements PairIterator<K, V> {

    /**
     * Iterator contains main data with deleted items. Can't contains
     * tombstones.
     */
    private final PairIterator<K, V> mainIterator;

    /**
     * Cached data iterator. Can contains tombstones.
     */
    private final Iterator<Pair<K, V>> deltaCacheIterator;

    private final TypeDescriptor<V> valueTypeDescriptor;

    private final Comparator<K> keyComparator;

    private Pair<K, V> nextMainIndexPair = null;
    private Pair<K, V> nextDeltaCachePair = null;

    public MergeDeltaCacheWithIndexIterator( //
            final PairIterator<K, V> mainIterator, //
            final TypeDescriptor<K> keyTypeDescriptor, //
            final TypeDescriptor<V> valueTypeDescriptor, //
            final List<Pair<K, V>> sortedDeltaCache) {
        this.mainIterator = Objects.requireNonNull(mainIterator);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        Objects.requireNonNull(keyTypeDescriptor);
        this.keyComparator = keyTypeDescriptor.getComparator();
        Objects.requireNonNull(sortedDeltaCache);
        this.deltaCacheIterator = sortedDeltaCache.iterator();
        nextMainIterator();
        nextCacheIterator();
        tryToRemoveTombstone();
    }

    public Pair<K, V> read() {
        if (hasNext()) {
            return next();
        } else {
            return null;
        }
    }

    @Override
    public void close() {
        mainIterator.close();
    }

    @Override
    public boolean hasNext() {
        return nextDeltaCachePair != null || nextMainIndexPair != null;
    }

    @Override
    public Pair<K, V> next() {
        if (nextMainIndexPair == null) {
            if (nextDeltaCachePair == null) {
                throw new NoSuchElementException("There no next element.");
            } else {
                final Pair<K, V> out = nextCacheIterator();
                tryToRemoveTombstone();
                return out;
            }
        } else {
            if (nextDeltaCachePair == null) {
                final Pair<K, V> out = nextMainIterator();
                tryToRemoveTombstone();
                return out;
            } else {
                // both next elements exists, so compare them
                final int cmp = keyComparator.compare(nextMainIndexPair.getKey(),
                        nextDeltaCachePair.getKey());
                if (cmp < 0) {
                    // main < cache
                    final Pair<K, V> out = nextMainIterator();
                    tryToRemoveTombstone();
                    return out;
                } else if (cmp == 0) {
                    // main = cache
                    final Pair<K, V> out = nextDeltaCachePair;
                    if (valueTypeDescriptor
                            .isTombstone(out.getValue())) {
                        nextMainIterator();
                        nextCacheIterator();
                        tryToRemoveTombstone();
                        if (hasNext()) {
                            return next();
                        } else {
                            return null;
                        }
                    } else {
                        nextMainIterator();
                        nextCacheIterator();
                        tryToRemoveTombstone();
                        return out;
                    }
                } else {
                    // main > cache
                    final Pair<K, V> out = nextCacheIterator();
                    tryToRemoveTombstone();
                    return out;
                }
            }
        }
    }

    private Pair<K, V> nextMainIterator() {
        final Pair<K, V> outPair = nextMainIndexPair;
        if (mainIterator.hasNext()) {
            nextMainIndexPair = mainIterator.next();
        } else {
            nextMainIndexPair = null;
        }
        return outPair;
    }

    private Pair<K, V> nextCacheIterator() {
        final Pair<K, V> outPair = nextDeltaCachePair;
        if (deltaCacheIterator.hasNext()) {
            nextDeltaCachePair = deltaCacheIterator.next();
        } else {
            nextDeltaCachePair = null;
        }
        return outPair;
    }

    private void tryToRemoveTombstone() {
        if (nextDeltaCachePair == null) {
            return;
        }
        if (nextMainIndexPair == null) {
            if (valueTypeDescriptor.isTombstone(nextDeltaCachePair.getValue())) {
                nextCacheIterator();
                tryToRemoveTombstone();
            }
            return;
        }
        if (valueTypeDescriptor.isTombstone(nextDeltaCachePair.getValue())) {
            final int cmp = keyComparator.compare(nextMainIndexPair.getKey(),
                    nextDeltaCachePair.getKey());
            if (cmp == 0) {
                nextMainIterator();
                nextCacheIterator();
                tryToRemoveTombstone();
            } else if (cmp > 0) {
                nextCacheIterator();
            }
        }
    }

}
