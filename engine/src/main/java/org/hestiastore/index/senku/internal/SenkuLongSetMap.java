package org.hestiastore.index.senku.internal;

import java.util.AbstractSet;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.LongConsumer;
import java.util.function.LongToIntFunction;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.senku.SenkuMergeFunction;
import org.hestiastore.index.senku.SenkuMergeFunctions;

/**
 * Exact, lazily allocated primitive set for an explicitly selected long-set
 * writer. Occupancy is independent from keys, so every long including zero is
 * representable. Generic map views exist only for compatibility and may box.
 */
final class SenkuLongSetMap extends SenkuIngestionMap<Long, NullValue> {

    private final int initialCapacity;
    private final LongToIntFunction hashFunction;
    private long[] keys = new long[0];
    private long[] occupied = new long[0];
    private int[] hashes = new int[0];
    private int resizeThreshold;
    private int size;
    private int modificationCount;

    /**
     * Creates an externally synchronized mutation-stripe set.
     *
     * @param requestedCapacity initial table capacity
     * @param hashFunction      primitive persistent-shard hash
     */
    SenkuLongSetMap(final int requestedCapacity,
            final LongToIntFunction hashFunction) {
        super(requestedCapacity,
                key -> hashFunction.applyAsInt(key.longValue()));
        initialCapacity = tableSizeFor(requestedCapacity);
        this.hashFunction = Vldtn.requireNonNull(hashFunction, "hashFunction");
    }

    /**
     * Adds one key using the already computed persistent-shard hash.
     *
     * @param key            exact primitive key
     * @param configuredHash configured shard hash
     * @return true only for a newly inserted key
     */
    boolean addLong(final long key, final int configuredHash) {
        if (keys.length == 0) {
            allocate(initialCapacity);
        }
        final int hash = tableHash(configuredHash);
        int slot = findSlot(key, hash, keys, hashes, occupied);
        if (isOccupied(occupied, slot)) {
            return false;
        }
        if (size >= resizeThreshold) {
            resize();
            slot = findSlot(key, hash, keys, hashes, occupied);
        }
        keys[slot] = key;
        hashes[slot] = hash;
        occupy(occupied, slot);
        size++;
        modificationCount++;
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public NullValue get(final Object key) {
        if (!(key instanceof Long)) {
            return null;
        }
        final long value = ((Long) key).longValue();
        return getWithHash(value, hashFunction.applyAsInt(value));
    }

    /** {@inheritDoc} */
    @Override
    NullValue getWithHash(final Long key, final int configuredHash) {
        if (keys.length == 0) {
            return null;
        }
        final int slot = findSlot(key.longValue(), tableHash(configuredHash),
                keys, hashes, occupied);
        return isOccupied(occupied, slot) ? NullValue.NULL : null;
    }

    /** {@inheritDoc} */
    @Override
    public NullValue put(final Long key, final NullValue value) {
        final long validatedKey = Vldtn.requireNonNull(key, "key").longValue();
        return putWithHash(validatedKey, value,
                hashFunction.applyAsInt(validatedKey));
    }

    /** {@inheritDoc} */
    @Override
    NullValue putWithHash(final Long key, final NullValue value,
            final int configuredHash) {
        requireSetValue(value);
        return addLong(Vldtn.requireNonNull(key, "key").longValue(),
                configuredHash) ? null : NullValue.NULL;
    }

    /** {@inheritDoc} */
    @Override
    boolean mergeWithHash(final Long key, final NullValue value,
            final int configuredHash,
            final SenkuMergeFunction<Long, NullValue> mergeFunction) {
        requireSetValue(value);
        if (mergeFunction != SenkuMergeFunctions.longSet()) {
            throw new IllegalArgumentException(
                    "Long sets require the explicit set reducer.");
        }
        return addLong(Vldtn.requireNonNull(key, "key").longValue(),
                configuredHash);
    }

    /** {@inheritDoc} */
    @Override
    public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override
    boolean isLongSet() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    void forEachLong(final LongConsumer consumer) {
        Vldtn.requireNonNull(consumer, "consumer");
        final int expected = modificationCount;
        for (int index = 0; index < keys.length; index++) {
            if (isOccupied(occupied, index)) {
                consumer.accept(keys[index]);
            }
        }
        checkModification(expected);
    }

    /** {@inheritDoc} */
    @Override
    public void forEach(
            final BiConsumer<? super Long, ? super NullValue> consumer) {
        Vldtn.requireNonNull(consumer, "consumer");
        forEachLong(key -> consumer.accept(key, NullValue.NULL));
    }

    /** {@inheritDoc} */
    @Override
    public Set<Map.Entry<Long, NullValue>> entrySet() {
        return new AbstractSet<>() {
            /** {@inheritDoc} */
            @Override
            public int size() {
                return SenkuLongSetMap.this.size;
            }

            /** {@inheritDoc} */
            @Override
            public Iterator<Map.Entry<Long, NullValue>> iterator() {
                return new EntryIterator();
            }
        };
    }

    private void allocate(final int capacity) {
        keys = new long[capacity];
        hashes = new int[capacity];
        occupied = new long[(capacity + 63) >>> 6];
        resizeThreshold = threshold(capacity);
    }

    private void resize() {
        if (keys.length == MAXIMUM_CAPACITY) {
            throw new IndexException(
                    "Senku ingestion map capacity is exhausted.");
        }
        final long[] oldKeys = keys;
        final int[] oldHashes = hashes;
        final long[] oldOccupied = occupied;
        allocate(keys.length << 1);
        for (int index = 0; index < oldKeys.length; index++) {
            if (isOccupied(oldOccupied, index)) {
                final int slot = findSlot(oldKeys[index], oldHashes[index],
                        keys, hashes, occupied);
                keys[slot] = oldKeys[index];
                hashes[slot] = oldHashes[index];
                occupy(occupied, slot);
            }
        }
    }

    private static int findSlot(final long key, final int hash,
            final long[] candidateKeys, final int[] candidateHashes,
            final long[] candidateOccupied) {
        final int mask = candidateKeys.length - 1;
        int slot = hash & mask;
        final int first = slot;
        while (isOccupied(candidateOccupied, slot)
                && (candidateHashes[slot] != hash
                        || candidateKeys[slot] != key)) {
            slot = slot + 1 & mask;
            if (slot == first) {
                throw new IndexException(
                        "Senku ingestion map has no empty probe slot.");
            }
        }
        return slot;
    }

    private static boolean isOccupied(final long[] bitmap, final int slot) {
        return (bitmap[slot >>> 6] & (1L << slot)) != 0;
    }

    private static void occupy(final long[] bitmap, final int slot) {
        bitmap[slot >>> 6] |= 1L << slot;
    }

    private static void requireSetValue(final NullValue value) {
        if (Vldtn.requireNonNull(value, "value") != NullValue.NULL) {
            throw new IllegalArgumentException(
                    "Long sets only accept NullValue.NULL.");
        }
    }

    private void checkModification(final int expected) {
        if (expected != modificationCount) {
            throw new ConcurrentModificationException();
        }
    }

    private final class EntryIterator
            implements Iterator<Map.Entry<Long, NullValue>> {
        private final int expected = modificationCount;
        private int nextIndex = findNext(0);

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {
            checkModification(expected);
            return nextIndex < keys.length;
        }

        /** {@inheritDoc} */
        @Override
        public Map.Entry<Long, NullValue> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final long key = keys[nextIndex];
            nextIndex = findNext(nextIndex + 1);
            return new SimpleImmutableEntry<>(key, NullValue.NULL);
        }

        private int findNext(final int start) {
            int index = start;
            while (index < keys.length && !isOccupied(occupied, index)) {
                index++;
            }
            return index;
        }
    }
}
