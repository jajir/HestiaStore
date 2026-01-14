package org.hestiastore.index.segment;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
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
class MergeWithCacheIterator<K, V> extends AbstractCloseableResource
        implements EntryIterator<K, V> {

    /**
     * Iterator contains main data with deleted items. Can't contains
     * tombstones.
     */
    private final EntryIterator<K, V> mainIterator;

    /**
     * Cached data iterator. Can contains tombstones.
     */
    private final Iterator<K> cacheKeyIterator;

    private final TypeDescriptor<V> valueTypeDescriptor;

    private final Comparator<K> keyComparator;

    private final Function<K, V> cacheValueGetter;

    private Entry<K, V> currentEntry = null;
    private Entry<K, V> nextMainPair = null;
    private K nextCacheKey = null;

    public MergeWithCacheIterator(final EntryIterator<K, V> mainIterator,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final List<K> sortedKeysFromCache,
            final Function<K, V> cacheValueGetter) {
        this.mainIterator = Vldtn.requireNonNull(mainIterator, "mainIterator");
        this.cacheKeyIterator = Vldtn
                .requireNonNull(sortedKeysFromCache, "sortedKeysFromCache")
                .iterator();
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        this.keyComparator = keyTypeDescriptor.getComparator();
        this.cacheValueGetter = Vldtn.requireNonNull(cacheValueGetter,
                "cacheValueGetter");

        nextMainIterator();
        nextCacheIterator();
        tryToremoveTombstone();
    }

    @Override
    public boolean hasNext() {
        return nextCacheKey != null || nextMainPair != null;
    }

    @Override
    public Entry<K, V> next() {
        if (nextMainPair == null) {
            if (nextCacheKey == null) {
                throw new NoSuchElementException("There no next element.");
            } else {
                currentEntry = nextCacheIterator();
                tryToremoveTombstone();
            }
        } else {
            if (nextCacheKey == null) {
                currentEntry = nextMainIterator();
                tryToremoveTombstone();
            } else {
                // both next elements exists
                currentEntry = nextBothIterators();
            }
        }
        return currentEntry;
    }

    private Entry<K, V> nextBothIterators() {
        final int cmp = keyComparator.compare(nextMainPair.getKey(),
                nextCacheKey);
        if (cmp < 0) {
            Entry<K, V> out = nextMainIterator();
            tryToremoveTombstone();
            return out;
        } else if (cmp == 0) {
            final Entry<K, V> nextCachePair = getCachedPair(nextCacheKey);
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
            Entry<K, V> out = nextCacheIterator();
            tryToremoveTombstone();
            return out;
        }
        return currentEntry;
    }

    private Entry<K, V> nextMainIterator() {
        final Entry<K, V> outPair = nextMainPair;
        if (mainIterator.hasNext()) {
            nextMainPair = mainIterator.next();
        } else {
            nextMainPair = null;
        }
        return outPair;
    }

    private Entry<K, V> nextCacheIterator() {
        final Entry<K, V> outPair = nextCacheKey == null ? null
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
        final Entry<K, V> nextCachePair = getCachedPair(nextCacheKey);
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

    private Entry<K, V> getCachedPair(final K cachedKey) {
        final V value = cacheValueGetter.apply(cachedKey);
        return Entry.of(cachedKey, value);
    }

    @Override
    protected void doClose() {
        mainIterator.close();
        cacheKeyIterator.forEachRemaining(i -> {
            // intentionally do nothing
        });
    }

}
