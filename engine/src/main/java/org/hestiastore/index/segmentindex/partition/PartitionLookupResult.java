package org.hestiastore.index.segmentindex.partition;

import org.hestiastore.index.Vldtn;

/**
 * Result of an overlay lookup inside a partition.
 *
 * @param <V> value type
 * @author honza
 */
public final class PartitionLookupResult<V> {

    private static final PartitionLookupResult<?> MISS = new PartitionLookupResult<>(
            false, null);

    private final boolean found;
    private final V value;

    private PartitionLookupResult(final boolean found, final V value) {
        this.found = found;
        this.value = value;
    }

    @SuppressWarnings("unchecked")
    public static <V> PartitionLookupResult<V> miss() {
        return (PartitionLookupResult<V>) MISS;
    }

    public static <V> PartitionLookupResult<V> hit(final V value) {
        return new PartitionLookupResult<>(true,
                Vldtn.requireNonNull(value, "value"));
    }

    public boolean isFound() {
        return found;
    }

    public V getValue() {
        return value;
    }
}
