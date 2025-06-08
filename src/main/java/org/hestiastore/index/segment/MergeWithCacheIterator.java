package org.hestiastore.index.segment;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.datatype.TypeDescriptor;

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
public class MergeWithCacheIterator<K, V> implements PairIterator<K, V> {

    /**
     * Iterator contains main data with deleted items. Can't contains
     * tombstones.
     */
    private final PairIterator<K, V> mainIterator;

    /**
     * Cached data iterator. Can contains tombstones.
     */
    private final Iterator<K> cacheKeyIterator;

    private final TypeDescriptor<V> valueTypeDescriptor;

    private final Comparator<K> keyComparator;

    private final Function<K, V> cacheValueGetter;

    private Pair<K, V> currentPair = null;
    private Pair<K, V> nextMainPair = null;
    private K nextCacheKey = null;

    public MergeWithCacheIterator(final PairIterator<K, V> mainIterator,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final List<K> sortedKeysFromCache,
            final Function<K, V> cacheValueGetter) {
        this.mainIterator = Objects.requireNonNull(mainIterator);
        this.cacheKeyIterator = Objects.requireNonNull(sortedKeysFromCache)
                .iterator();
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        Objects.requireNonNull(keyTypeDescriptor);
        this.keyComparator = keyTypeDescriptor.getComparator();
        this.cacheValueGetter = Objects.requireNonNull(cacheValueGetter);

        nextMainIterator();
        nextCacheIterator();
        tryToremoveTombstone();
    }

    @Override
    public boolean hasNext() {
        return nextCacheKey != null || nextMainPair != null;
    }

    @Override
    public Pair<K, V> next() {
        if (nextMainPair == null) {
            if (nextCacheKey == null) {
                throw new NoSuchElementException("There no next element.");
            } else {
                currentPair = nextCacheIterator();
                tryToremoveTombstone();
            }
        } else {
            if (nextCacheKey == null) {
                currentPair = nextMainIterator();
                tryToremoveTombstone();
            } else {
                // both next elements exists
                currentPair = nextBothIterators();
            }
        }
        return currentPair;
    }

    private Pair<K, V> nextBothIterators() {
        final int cmp = keyComparator.compare(nextMainPair.getKey(),
                nextCacheKey);
        if (cmp < 0) {
            Pair<K, V> out = nextMainIterator();
            tryToremoveTombstone();
            return out;
        } else if (cmp == 0) {
            final Pair<K, V> nextCachePair = getCachedPair(nextCacheKey);
            if (valueTypeDescriptor.isTombstone(nextCachePair.getValue())) {
                nextMainIterator();
                nextCacheIterator();
                tryToremoveTombstone();
                next();
            } else {
                return nextCachePair;
            }
            nextMainIterator();
            nextCacheIterator();
            tryToremoveTombstone();
        } else {
            Pair<K, V> out = nextCacheIterator();
            tryToremoveTombstone();
            return out;
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
        final Pair<K, V> outPair = nextCacheKey == null ? null
                : getCachedPair(nextCacheKey);
        if (cacheKeyIterator.hasNext()) {
            nextCacheKey = cacheKeyIterator.next();
        } else {
            nextCacheKey = null;
        }
        return outPair;
    }

    private void tryToremoveTombstone() {
        if (nextCacheKey == null) {
            return;
        }
        final Pair<K, V> nextCachePair = getCachedPair(nextCacheKey);
        if (nextMainPair == null) {
            if (valueTypeDescriptor.isTombstone(nextCachePair.getValue())) {
                nextCacheIterator();
                tryToremoveTombstone();
            }
            return;
        }
        if (valueTypeDescriptor.isTombstone(nextCachePair.getValue())) {
            final int cmp = keyComparator.compare(nextMainPair.getKey(),
                    nextCacheKey);
            if (cmp == 0) {
                nextMainIterator();
                nextCacheIterator();
                tryToremoveTombstone();
            } else if (cmp > 0) {
                nextCacheIterator();
            }
        }
    }

    private Pair<K, V> getCachedPair(final K cachedKey) {
        final V value = cacheValueGetter.apply(cachedKey);
        return Pair.of(cachedKey, value);
    }

    @Override
    public void close() {
        mainIterator.close();
        cacheKeyIterator.forEachRemaining(i -> {
            // intentionally do nothing
        });
    }

}
