package org.hestiastore.index.senku.internal;

import org.hestiastore.index.Vldtn;

/**
 * Worker-confined selector of cached encoded keys and stable source ordinals.
 * One entry per live source is ordered by signed key, then source ordinal.
 * Updating the selected source needs one downward heap repair, without cursor
 * dereferences, boxing, or removing and reinserting the same source. The merge
 * writer owns all cursors; this component owns no readers or other resources.
 */
final class SenkuLongMergeHeap {

    private final long[] keys;
    private final int[] ordinals;
    private int size;

    /** Creates bounded storage for at most the given number of sources. */
    SenkuLongMergeHeap(final int capacity) {
        Vldtn.requireGreaterThanOrEqualToZero(capacity, "capacity");
        keys = new long[capacity];
        ordinals = new int[capacity];
    }

    /**
     * Adds an initial live source. The caller supplies each source ordinal at
     * most once, in the range zero through capacity minus one.
     */
    void add(final long key, final int ordinal) {
        Vldtn.requireTrue(ordinal >= 0 && ordinal < keys.length,
                "Source ordinal must be within the selector capacity");
        Vldtn.requireTrue(size < keys.length, "Merge selector is full");
        int index = size;
        while (index > 0) {
            final int parent = (index - 1) >>> 1;
            if (!comesBefore(key, ordinal, keys[parent], ordinals[parent])) {
                break;
            }
            keys[index] = keys[parent];
            ordinals[index] = ordinals[parent];
            index = parent;
        }
        keys[index] = key;
        ordinals[index] = ordinal;
        size++;
    }

    /** @return whether no source has a current record */
    boolean isEmpty() {
        return size == 0;
    }

    /** @return the smallest current encoded key */
    long key() {
        requireRoot();
        return keys[0];
    }

    /** @return the earliest source ordinal having the smallest current key */
    int ordinal() {
        requireRoot();
        return ordinals[0];
    }

    /**
     * Replaces only the selected source's key after it advances. A root update
     * can only need downward repair, even if the replacement key is smaller.
     * This selector does not change the cursor's input-validation contract.
     */
    void replaceRoot(final long key) {
        requireRoot();
        siftDown(key, ordinals[0]);
    }

    /** Removes the selected exhausted source without reading its cursor. */
    void removeRoot() {
        requireRoot();
        if (--size > 0) {
            siftDown(keys[size], ordinals[size]);
        }
    }

    /** Discards cached selection state after the writer closes all inputs. */
    void clear() {
        size = 0;
    }

    private void siftDown(final long key, final int ordinal) {
        int index = 0;
        final int firstLeaf = size >>> 1;
        while (index < firstLeaf) {
            int child = (index << 1) + 1;
            final int right = child + 1;
            if (right < size && comesBefore(keys[right], ordinals[right],
                    keys[child], ordinals[child])) {
                child = right;
            }
            if (!comesBefore(keys[child], ordinals[child], key, ordinal)) {
                break;
            }
            keys[index] = keys[child];
            ordinals[index] = ordinals[child];
            index = child;
        }
        keys[index] = key;
        ordinals[index] = ordinal;
    }

    private void requireRoot() {
        if (size == 0) {
            throw new IllegalStateException("Merge selector is empty");
        }
    }

    private static boolean comesBefore(final long firstKey,
            final int firstOrdinal, final long secondKey,
            final int secondOrdinal) {
        return firstKey < secondKey
                || (firstKey == secondKey && firstOrdinal < secondOrdinal);
    }
}
