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

/**
 * Merges the on-disk index iterator with the in-memory delta cache view.
 * Prefers delta cache entries for duplicate keys and skips tombstones.
 *
 * @param <K> key type
 * @param <V> value type
 */
class MergeDeltaCacheWithIndexIterator<K, V>
        extends AbstractCloseableResource implements EntryIterator<K, V> {

    private final EntryIterator<K, V> mainIterator;
    private final Iterator<Entry<K, V>> deltaCacheIterator;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final Comparator<K> keyComparator;

    private Entry<K, V> next = null;

    private Entry<K, V> mainCurrent;
    private Entry<K, V> deltaCurrent;

    /**
     * Creates a merged iterator over index data and delta cache entries.
     *
     * @param mainIterator iterator over the index file
     * @param keyTypeDescriptor key type descriptor used for ordering
     * @param valueTypeDescriptor value type descriptor (for tombstones)
     * @param sortedDeltaCache delta cache entries sorted by key
     */
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

    /**
     * Returns whether another merged entry is available.
     *
     * @return true when another entry exists
     */
    @Override
    public boolean hasNext() {
        return next != null;
    }

    /**
     * Returns the next merged entry, skipping tombstones.
     *
     * @return next entry in key order
     */
    @Override
    public Entry<K, V> next() {
        if (next == null) {
            throw new NoSuchElementException("No next element.");
        }
        Entry<K, V> result = next;
        advance();
        return result;
    }

    /**
     * Advances to the next non-tombstone candidate across both sources.
     */
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

    /**
     * Reads the next entry from the index iterator.
     *
     * @return next index entry or null if exhausted
     */
    private Entry<K, V> readNextPairFromMain() {
        if (mainIterator.hasNext()) {
            return mainIterator.next();
        } else {
            return null;
        }
    }

    /**
     * Reads the next entry from the delta cache iterator.
     *
     * @return next delta entry or null if exhausted
     */
    private Entry<K, V> readNextPairFromCache() {
        if (deltaCacheIterator.hasNext()) {
            return deltaCacheIterator.next();
        } else {
            return null;
        }
    }

    /**
     * Closes the underlying index iterator.
     */
    @Override
    protected void doClose() {
        mainIterator.close();
    }
}
