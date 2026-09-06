package org.hestiastore.index.senku.internal;

import java.util.Arrays;
import java.util.Comparator;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;

/**
 * Compact, in-place sortable storage used while preparing one Senku flush.
 *
 * <p>
 * Exact {@link TypeDescriptorLong} keys use a primitive array. Other key types
 * retain their descriptor-defined ordering in a generic object array. Exact
 * {@link TypeDescriptorNull} values require no per-entry storage because both
 * marker values have the same zero-byte on-disk representation.
 * </p>
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SenkuFlushOrder<K, V> {

    private static final int INSERTION_SORT_THRESHOLD = 16;

    private final Comparator<K> keyComparator;
    private final long[] longKeys;
    private final Object[] objectKeys;
    private final Object[] values;

    /**
     * Creates exactly sized ordering storage without retaining map entries.
     *
     * @param keyTypeDescriptor   key descriptor
     * @param valueTypeDescriptor value descriptor
     * @param size                entry count
     */
    SenkuFlushOrder(final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor, final int size) {
        final TypeDescriptor<K> keys = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        final TypeDescriptor<V> valueDescriptor = Vldtn
                .requireNonNull(valueTypeDescriptor, "valueTypeDescriptor");
        final int validatedSize = Vldtn.requireGreaterThanOrEqualToZero(size,
                "size");
        keyComparator = keys.getComparator();
        if (keys.getClass() == TypeDescriptorLong.class) {
            longKeys = new long[validatedSize];
            objectKeys = null;
        } else {
            longKeys = null;
            objectKeys = new Object[validatedSize];
        }
        values = valueDescriptor.getClass() == TypeDescriptorNull.class ? null
                : new Object[validatedSize];
    }

    /**
     * Stores one mapping at its shard-partitioned position.
     *
     * @param index destination index
     * @param key   key
     * @param value value
     */
    void set(final int index, final K key, final V value) {
        if (hasPrimitiveLongKeys()) {
            longKeys[index] = ((Long) key).longValue();
        } else {
            objectKeys[index] = key;
        }
        if (values != null) {
            values[index] = value;
        }
    }

    /**
     * Stores one key from an explicitly selected primitive long set.
     *
     * @param index destination index
     * @param key   primitive key
     */
    void setLong(final int index, final long key) {
        if (longKeys == null || values != null) {
            throw new IllegalStateException(
                    "Primitive long set is not available.");
        }
        longKeys[index] = key;
    }

    /**
     * Sorts one half-open shard range in place while keeping values paired.
     *
     * @param fromInclusive first index
     * @param toExclusive   index after the last entry
     */
    void sort(final int fromInclusive, final int toExclusive) {
        if (toExclusive - fromInclusive < 2) {
            return;
        }
        if (hasPrimitiveLongKeys()) {
            if (values == null) {
                Arrays.sort(longKeys, fromInclusive, toExclusive);
                return;
            }
            sortLongs(fromInclusive, toExclusive - 1,
                    depthLimit(toExclusive - fromInclusive));
        } else {
            sortObjects(fromInclusive, toExclusive - 1,
                    depthLimit(toExclusive - fromInclusive));
        }
    }

    /**
     * Returns whether this order uses the primitive long-key path.
     *
     * @return {@code true} for exact {@link TypeDescriptorLong} keys
     */
    boolean hasPrimitiveLongKeys() {
        return longKeys != null;
    }

    /**
     * Returns a primitive key from a specialized long order.
     *
     * @param index entry index
     * @return key
     */
    long longKey(final int index) {
        if (!hasPrimitiveLongKeys()) {
            throw new IllegalStateException(
                    "Primitive long keys are not available.");
        }
        return longKeys[index];
    }

    /**
     * Returns a key from the generic order.
     *
     * @param index entry index
     * @return key
     */
    @SuppressWarnings("unchecked")
    K key(final int index) {
        if (hasPrimitiveLongKeys()) {
            throw new IllegalStateException("Generic keys are not available.");
        }
        return (K) objectKeys[index];
    }

    /**
     * Returns the value paired with an ordered key.
     *
     * @param index entry index
     * @return value, or the canonical null marker for a zero-byte value type
     */
    @SuppressWarnings("unchecked")
    V value(final int index) {
        if (values == null) {
            return (V) NullValue.NULL;
        }
        return (V) values[index];
    }

    private void sortLongs(final int initialLow, final int initialHigh,
            final int initialDepthLimit) {
        int low = initialLow;
        int high = initialHigh;
        int remainingDepth = initialDepthLimit;
        while (high - low > INSERTION_SORT_THRESHOLD) {
            if (remainingDepth == 0) {
                heapSortLongs(low, high);
                return;
            }
            remainingDepth--;
            int left = low;
            int right = high;
            final long pivot = longKeys[(low + high) >>> 1];
            while (left <= right) {
                while (longKeys[left] < pivot) {
                    left++;
                }
                while (longKeys[right] > pivot) {
                    right--;
                }
                if (left <= right) {
                    swapLongs(left++, right--);
                }
            }
            if (right - low < high - left) {
                if (low < right) {
                    sortLongs(low, right, remainingDepth);
                }
                low = left;
            } else {
                if (left < high) {
                    sortLongs(left, high, remainingDepth);
                }
                high = right;
            }
        }
        insertionSortLongs(low, high);
    }

    private void insertionSortLongs(final int low, final int high) {
        for (int index = low + 1; index <= high; index++) {
            int current = index;
            while (current > low && longKeys[current - 1] > longKeys[current]) {
                swapLongs(current - 1, current);
                current--;
            }
        }
    }

    private void heapSortLongs(final int low, final int high) {
        final int size = high - low + 1;
        for (int root = size / 2 - 1; root >= 0; root--) {
            siftDownLongs(low, root, size);
        }
        for (int end = size - 1; end > 0; end--) {
            swapLongs(low, low + end);
            siftDownLongs(low, 0, end);
        }
    }

    private void siftDownLongs(final int low, final int initialRoot,
            final int size) {
        int root = initialRoot;
        int child = 2 * root + 1;
        while (child < size) {
            if (child + 1 < size
                    && longKeys[low + child] < longKeys[low + child + 1]) {
                child++;
            }
            if (longKeys[low + root] >= longKeys[low + child]) {
                return;
            }
            swapLongs(low + root, low + child);
            root = child;
            child = 2 * root + 1;
        }
    }

    private void swapLongs(final int first, final int second) {
        if (first == second) {
            return;
        }
        final long key = longKeys[first];
        longKeys[first] = longKeys[second];
        longKeys[second] = key;
        swapValues(first, second);
    }

    private void sortObjects(final int initialLow, final int initialHigh,
            final int initialDepthLimit) {
        int low = initialLow;
        int high = initialHigh;
        int remainingDepth = initialDepthLimit;
        while (high - low > INSERTION_SORT_THRESHOLD) {
            if (remainingDepth == 0) {
                heapSortObjects(low, high);
                return;
            }
            remainingDepth--;
            int left = low;
            int right = high;
            final K pivot = objectKey((low + high) >>> 1);
            while (left <= right) {
                while (keyComparator.compare(objectKey(left), pivot) < 0) {
                    left++;
                }
                while (keyComparator.compare(objectKey(right), pivot) > 0) {
                    right--;
                }
                if (left <= right) {
                    swapObjects(left++, right--);
                }
            }
            if (right - low < high - left) {
                if (low < right) {
                    sortObjects(low, right, remainingDepth);
                }
                low = left;
            } else {
                if (left < high) {
                    sortObjects(left, high, remainingDepth);
                }
                high = right;
            }
        }
        insertionSortObjects(low, high);
    }

    private void insertionSortObjects(final int low, final int high) {
        for (int index = low + 1; index <= high; index++) {
            int current = index;
            while (current > low && keyComparator
                    .compare(objectKey(current - 1), objectKey(current)) > 0) {
                swapObjects(current - 1, current);
                current--;
            }
        }
    }

    private void heapSortObjects(final int low, final int high) {
        final int size = high - low + 1;
        for (int root = size / 2 - 1; root >= 0; root--) {
            siftDownObjects(low, root, size);
        }
        for (int end = size - 1; end > 0; end--) {
            swapObjects(low, low + end);
            siftDownObjects(low, 0, end);
        }
    }

    private void siftDownObjects(final int low, final int initialRoot,
            final int size) {
        int root = initialRoot;
        int child = 2 * root + 1;
        while (child < size) {
            if (child + 1 < size
                    && keyComparator.compare(objectKey(low + child),
                            objectKey(low + child + 1)) < 0) {
                child++;
            }
            if (keyComparator.compare(objectKey(low + root),
                    objectKey(low + child)) >= 0) {
                return;
            }
            swapObjects(low + root, low + child);
            root = child;
            child = 2 * root + 1;
        }
    }

    private void swapObjects(final int first, final int second) {
        if (first == second) {
            return;
        }
        final Object key = objectKeys[first];
        objectKeys[first] = objectKeys[second];
        objectKeys[second] = key;
        swapValues(first, second);
    }

    private void swapValues(final int first, final int second) {
        if (values == null) {
            return;
        }
        final Object value = values[first];
        values[first] = values[second];
        values[second] = value;
    }

    @SuppressWarnings("unchecked")
    private K objectKey(final int index) {
        return (K) objectKeys[index];
    }

    private static int depthLimit(final int length) {
        return 2 * (Integer.SIZE - 1 - Integer.numberOfLeadingZeros(length));
    }
}
