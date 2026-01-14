package org.hestiastore.index.segment;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;

class SegmentDeltaCacheEntryIterator<K, V> extends AbstractCloseableResource
        implements EntryIterator<K, V> {

    private final SegmentDeltaCacheController<K, V> deltaCacheController;
    private final Iterator<K> keyIterator;
    private K currentKey;

    SegmentDeltaCacheEntryIterator(final List<K> sortedKeys,
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
    public Entry<K, V> next() {
        currentKey = keyIterator.next();
        if (currentKey == null) {
            throw new NoSuchElementException();
        }
        return getCurrentPair();
    }

    @Override
    protected void doClose() {
        keyIterator.forEachRemaining(i -> {
            // intentionally do nothing, just move forward
        });
        currentKey = null;
    }

    private SegmentDeltaCache<K, V> getDeltaSegmentCache() {
        return deltaCacheController.getDeltaCache();
    }

    private Entry<K, V> getCurrentPair() {
        final V value = getDeltaSegmentCache().get(currentKey);
        if (value == null) {
            throw new IllegalStateException("Inconsistent delta cache state.");
        }
        return Entry.of(currentKey, value);
    }

}
