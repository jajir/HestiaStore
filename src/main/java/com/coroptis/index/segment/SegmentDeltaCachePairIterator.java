package com.coroptis.index.segment;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;

public class SegmentDeltaCachePairIterator<K, V> implements PairIterator<K, V> {

    private final SegmentDeltaCacheController<K, V> deltaCacheController;
    private final Iterator<K> keyIterator;
    private K currentKey;

    SegmentDeltaCachePairIterator(final List<K> sortedKeys,
            final SegmentDeltaCacheController<K, V> deltaCacheController) {
        keyIterator = sortedKeys.iterator();
        this.deltaCacheController = Objects
                .requireNonNull(deltaCacheController);
        currentKey = null;
    }

    @Override
    public boolean hasNext() {
        return keyIterator.hasNext();
    }

    @Override
    public Pair<K, V> next() {
        currentKey = keyIterator.next();
        if (currentKey == null) {
            throw new NoSuchElementException();
        }
        return getCurrentPair();
    }

    @Override
    public void close() {
        keyIterator.forEachRemaining(i -> {
            // intentionally do nothing, just move forward
        });
        currentKey = null;
    }

    private SegmentDeltaCache<K, V> getDeltaSegmentCache() {
        return deltaCacheController.getDeltaCache();
    }

    private Pair<K, V> getCurrentPair() {
        final V value = getDeltaSegmentCache().get(currentKey);
        if (value == null) {
            throw new IllegalStateException("Inconsistent delta cache state.");
        }
        return Pair.of(currentKey, value);
    }

}
