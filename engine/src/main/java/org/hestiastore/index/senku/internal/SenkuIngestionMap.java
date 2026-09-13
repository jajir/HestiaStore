package org.hestiastore.index.senku.internal;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.LongConsumer;
import java.util.function.ToIntFunction;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.senku.SenkuMergeFunction;

/**
 * Bounded open-addressed map used by one Senku mutation stripe. The configured
 * shard hash is independently avalanched before probing, so collision-heavy key
 * hash codes do not create {@link java.util.HashMap} tree bins. Keys and values
 * are stored in flat arrays without one node allocation per mapping.
 * <p>
 * The map accepts non-null keys and values and does not support removal.
 * Callers provide external synchronization while a batch is mutable; detached
 * batches can be traversed without synchronization.
 * </p>
 *
 * @param <K> key type
 * @param <V> value type
 */
class SenkuIngestionMap<K, V> extends AbstractMap<K, V> {

    static final int MAXIMUM_CAPACITY = 1 << 30;
    private static final int MINIMUM_CAPACITY = 4;
    private static final int LOAD_FACTOR_NUMERATOR = 3;
    private static final int LOAD_FACTOR_DENOMINATOR = 4;
    private static final Object[] EMPTY_OBJECTS = new Object[0];
    private static final int[] EMPTY_HASHES = new int[0];
    private static final int TABLE_HASH_SEED = 0x9e37_79b9;
    private static final int TABLE_MIX_MULTIPLIER_1 = 0x85eb_ca6b;
    private static final int TABLE_MIX_MULTIPLIER_2 = 0xc2b2_ae35;

    private final ToIntFunction<K> hashFunction;
    private final int initialCapacity;

    private Object[] keys = EMPTY_OBJECTS;
    private Object[] values = EMPTY_OBJECTS;
    private int[] hashes = EMPTY_HASHES;
    private int resizeThreshold;
    private int size;
    private int modificationCount;

    /**
     * Creates one lazily allocated mutation-stripe map.
     *
     * @param requestedCapacity expected table capacity before power-of-two
     *                          normalization
     * @param hashFunction      configured persistent-shard hash function
     */
    SenkuIngestionMap(final int requestedCapacity,
            final ToIntFunction<K> hashFunction) {
        initialCapacity = tableSizeFor(Vldtn.requireGreaterThanZero(
                requestedCapacity, "requestedCapacity"));
        this.hashFunction = Vldtn.requireNonNull(hashFunction, "hashFunction");
    }

    /** {@inheritDoc} */
    @Override
    public V get(final Object key) {
        if (key == null || keys.length == 0) {
            return null;
        }
        @SuppressWarnings("unchecked")
        final K typedKey = (K) key;
        return getWithHash(typedKey, hashFunction.applyAsInt(typedKey));
    }

    /**
     * Finds a key using a hash already computed by the ingestor.
     *
     * @param key            non-null key
     * @param configuredHash configured persistent-shard hash
     *
     * @return current value, or null when absent
     */
    V getWithHash(final K key, final int configuredHash) {
        if (keys.length == 0) {
            return null;
        }
        final int tableHash = tableHash(configuredHash);
        final int slot = findSlot(key, tableHash, keys, hashes);
        return keys[slot] == null ? null : valueAt(slot);
    }

    /** {@inheritDoc} */
    @Override
    public V put(final K key, final V value) {
        final K validatedKey = Vldtn.requireNonNull(key, "key");
        return putWithHash(validatedKey, Vldtn.requireNonNull(value, "value"),
                hashFunction.applyAsInt(validatedKey));
    }

    /**
     * Adds or replaces a mapping using a hash already computed by the ingestor.
     *
     * @param key            non-null key
     * @param value          non-null value
     * @param configuredHash configured persistent-shard hash
     *
     * @return previous value, or null when the key was absent
     */
    V putWithHash(final K key, final V value, final int configuredHash) {
        ensureAllocated();
        final int tableHash = tableHash(configuredHash);
        int slot = findSlot(key, tableHash, keys, hashes);
        if (keys[slot] != null) {
            final V previous = valueAt(slot);
            values[slot] = value;
            return previous;
        }
        if (size >= resizeThreshold) {
            resize();
            slot = findSlot(key, tableHash, keys, hashes);
        }
        keys[slot] = key;
        values[slot] = value;
        hashes[slot] = tableHash;
        size++;
        modificationCount++;
        return null;
    }

    /**
     * Inserts or reduces one mapping with one probe in the normal case. A
     * failing reducer leaves the existing value untouched.
     *
     * @param key            non-null logical key
     * @param value          non-null incoming value
     * @param configuredHash previously computed shard hash
     * @param mergeFunction  duplicate reducer
     *
     * @return true when a distinct key was inserted
     */
    boolean mergeWithHash(final K key, final V value, final int configuredHash,
            final SenkuMergeFunction<K, V> mergeFunction) {
        ensureAllocated();
        final int tableHash = tableHash(configuredHash);
        int slot = findSlot(key, tableHash, keys, hashes);
        if (keys[slot] != null) {
            final V merged;
            try {
                merged = Vldtn.requireNonNull(
                        mergeFunction.apply(key, valueAt(slot), value),
                        "mergedValue");
            } catch (Exception e) {
                if (e instanceof IndexException) {
                    throw (IndexException) e;
                }
                throw new IndexException("Senku merge function failed.", e);
            }
            values[slot] = merged;
            return false;
        }
        if (size >= resizeThreshold) {
            resize();
            slot = findSlot(key, tableHash, keys, hashes);
        }
        keys[slot] = key;
        values[slot] = value;
        hashes[slot] = tableHash;
        size++;
        modificationCount++;
        return true;
    }

    /** @return whether this batch explicitly stores a primitive long set */
    boolean isLongSet() {
        return false;
    }

    /**
     * Traverses an explicitly selected primitive long set without boxing.
     *
     * @param consumer primitive key consumer
     */
    void forEachLong(final LongConsumer consumer) {
        throw new IllegalStateException("Primitive long set is not available.");
    }

    /**
     * Traverses primitive set keys and cached configured hashes without boxing.
     *
     * @param consumer primitive key/hash consumer
     */
    void forEachLongWithHash(final SenkuLongKeyHashConsumer consumer) {
        throw new IllegalStateException("Primitive long set is not available.");
    }

    /** {@inheritDoc} */
    @Override
    public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return new AbstractSet<>() {
            /** {@inheritDoc} */
            @Override
            public Iterator<Map.Entry<K, V>> iterator() {
                return new EntryIterator();
            }

            /** {@inheritDoc} */
            @Override
            public int size() {
                return SenkuIngestionMap.this.size;
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public void forEach(final BiConsumer<? super K, ? super V> consumer) {
        Vldtn.requireNonNull(consumer, "consumer");
        final int expectedModificationCount = modificationCount;
        for (int index = 0; index < keys.length; index++) {
            if (keys[index] != null) {
                consumer.accept(keyAt(index), valueAt(index));
            }
        }
        if (expectedModificationCount != modificationCount) {
            throw new ConcurrentModificationException();
        }
    }

    /**
     * Independently avalanches the configured hash for table probing. This must
     * remain independent from mutation-stripe selection because every entry in
     * one stripe shares the selector's low bits.
     *
     * @param configuredHash configured persistent-shard hash
     *
     * @return mixed table hash
     */
    static int tableHash(final int configuredHash) {
        int mixed = configuredHash ^ TABLE_HASH_SEED;
        mixed ^= mixed >>> 16;
        mixed *= TABLE_MIX_MULTIPLIER_1;
        mixed ^= mixed >>> 13;
        mixed *= TABLE_MIX_MULTIPLIER_2;
        return mixed ^ mixed >>> 16;
    }

    private void ensureAllocated() {
        if (keys.length != 0) {
            return;
        }
        keys = new Object[initialCapacity];
        values = new Object[initialCapacity];
        hashes = new int[initialCapacity];
        resizeThreshold = threshold(initialCapacity);
    }

    private void resize() {
        if (keys.length == MAXIMUM_CAPACITY) {
            throw new IndexException(
                    "Senku ingestion map capacity is exhausted.");
        }
        final int nextCapacity = keys.length << 1;
        final Object[] nextKeys = new Object[nextCapacity];
        final Object[] nextValues = new Object[nextCapacity];
        final int[] nextHashes = new int[nextCapacity];
        for (int index = 0; index < keys.length; index++) {
            final Object key = keys[index];
            if (key != null) {
                final int slot = findSlot(key, hashes[index], nextKeys,
                        nextHashes);
                nextKeys[slot] = key;
                nextValues[slot] = values[index];
                nextHashes[slot] = hashes[index];
            }
        }
        keys = nextKeys;
        values = nextValues;
        hashes = nextHashes;
        resizeThreshold = threshold(nextCapacity);
    }

    /**
     * Finds an existing or empty slot and fails deterministically if a broken
     * capacity invariant ever exposes a completely full table.
     *
     * @param key             key being located
     * @param tableHash       independently mixed table hash
     * @param candidateKeys   power-of-two key table
     * @param candidateHashes hash table parallel to {@code candidateKeys}
     *
     * @return matching or empty slot
     */
    static int findSlot(final Object key, final int tableHash,
            final Object[] candidateKeys, final int[] candidateHashes) {
        final int mask = candidateKeys.length - 1;
        int slot = tableHash & mask;
        final int firstSlot = slot;
        while (candidateKeys[slot] != null
                && (candidateHashes[slot] != tableHash
                        || !key.equals(candidateKeys[slot]))) {
            slot = slot + 1 & mask;
            if (slot == firstSlot) {
                throw new IndexException(
                        "Senku ingestion map has no empty probe slot.");
            }
        }
        return slot;
    }

    /**
     * Calculates the bounded load threshold shared by both ingestion layouts.
     *
     * @param capacity normalized table capacity
     *
     * @return maximum size before growth is needed
     */
    static int threshold(final int capacity) {
        if (capacity == MAXIMUM_CAPACITY) {
            return MAXIMUM_CAPACITY - 1;
        }
        return capacity / LOAD_FACTOR_DENOMINATOR * LOAD_FACTOR_NUMERATOR;
    }

    /**
     * Normalizes a validated requested capacity to the supported power of two.
     *
     * @param requestedCapacity positive requested table capacity
     *
     * @return bounded normalized capacity
     */
    static int tableSizeFor(final int requestedCapacity) {
        final int bounded = Math.max(MINIMUM_CAPACITY, requestedCapacity);
        if (bounded >= MAXIMUM_CAPACITY) {
            return MAXIMUM_CAPACITY;
        }
        return Integer.highestOneBit(bounded - 1) << 1;
    }

    @SuppressWarnings("unchecked")
    private K keyAt(final int index) {
        return (K) keys[index];
    }

    @SuppressWarnings("unchecked")
    private V valueAt(final int index) {
        return (V) values[index];
    }

    private final class EntryIterator implements Iterator<Map.Entry<K, V>> {

        private final int expectedModificationCount = modificationCount;
        private int nextIndex = findNext(0);

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {
            requireUnmodified();
            return nextIndex < keys.length;
        }

        /** {@inheritDoc} */
        @Override
        public Map.Entry<K, V> next() {
            requireUnmodified();
            if (nextIndex >= keys.length) {
                throw new NoSuchElementException();
            }
            final int currentIndex = nextIndex;
            nextIndex = findNext(currentIndex + 1);
            return new SimpleImmutableEntry<>(keyAt(currentIndex),
                    valueAt(currentIndex));
        }

        private int findNext(final int start) {
            int candidate = start;
            while (candidate < keys.length && keys[candidate] == null) {
                candidate++;
            }
            return candidate;
        }

        private void requireUnmodified() {
            if (expectedModificationCount != modificationCount) {
                throw new ConcurrentModificationException();
            }
        }
    }
}
