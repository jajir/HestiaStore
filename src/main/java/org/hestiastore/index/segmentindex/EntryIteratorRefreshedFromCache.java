package org.hestiastore.index.segmentindex;

import java.util.NoSuchElementException;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.cache.UniqueCache;
import org.hestiastore.index.datatype.TypeDescriptor;

public class EntryIteratorRefreshedFromCache<K, V>
        extends AbstractCloseableResource implements EntryIterator<K, V> {

    private final EntryIterator<K, V> entryIterator;
    private final UniqueCache<K, V> cache;
    private final UniqueCache<K, V> flushingCache;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private Entry<K, V> currentEntry = null;

    EntryIteratorRefreshedFromCache(final EntryIterator<K, V> entryIterator,
            final UniqueCache<K, V> cache,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this(entryIterator, cache, null, valueTypeDescriptor);
    }

    EntryIteratorRefreshedFromCache(final EntryIterator<K, V> entryIterator,
            final UniqueCache<K, V> cache,
            final UniqueCache<K, V> flushingCache,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.entryIterator = Vldtn.requireNonNull(entryIterator, "entryIterator");
        this.cache = Vldtn.requireNonNull(cache, "cache");
        this.flushingCache = flushingCache;
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        currentEntry = readNext();
    }

    @Override
    public boolean hasNext() {
        return currentEntry != null;
    }

    @Override
    public Entry<K, V> next() {
        if (currentEntry == null) {
            throw new NoSuchElementException("No more elements");
        }
        final Entry<K, V> entry = currentEntry;
        currentEntry = readNext();
        return entry;
    }

    @Override
    protected void doClose() {
        entryIterator.close();
    }

    private Entry<K, V> readNext() {
        while (true) {
            if (!entryIterator.hasNext()) {
                return null;
            }
            final Entry<K, V> entry = entryIterator.next();
            final V value = getCachedValue(entry.getKey());
            if (value == null) {
                return entry;
            }
            if (!valueTypeDescriptor.isTombstone(value)) {
                return Entry.of(entry.getKey(), value);
            }
        }
    }

    private V getCachedValue(final K key) {
        final V value = cache.get(key);
        if (value != null) {
            return value;
        }
        if (flushingCache == null) {
            return null;
        }
        return flushingCache.get(key);
    }

}
