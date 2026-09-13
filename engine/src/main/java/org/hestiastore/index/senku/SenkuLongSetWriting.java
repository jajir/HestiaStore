package org.hestiastore.index.senku;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.NullValue;

/**
 * Thread-safe, explicitly selected pure long-set writer. Primitive puts share
 * the generic writer's admission, rotation, backpressure and finish lifecycle.
 */
public interface SenkuLongSetWriting extends SenkuWriting<Long, NullValue> {

    /**
     * Adds one exact primitive key; repeated keys have no further effect.
     *
     * @param key any signed long key, including zero
     */
    void putLong(long key);

    /**
     * Synchronously adds a slice of exact keys using bounded grouped ingestion.
     * The complete array range is validated before any key is accepted. A valid
     * empty slice is a no-op while writing, but is rejected after finishing or
     * failure just like a nonempty slice.
     * <p>
     * This is not a transaction: a routing, interruption, flush, or concurrent
     * finish failure can leave a subset accepted, not necessarily an input
     * prefix. Failures use the same lifecycle handling as
     * {@link #putLong(long)}; racing with finish rejects remaining keys without
     * undoing accepted keys. The caller must not modify the slice during the
     * call; the array is not retained after return and can then be reused.
     * </p>
     *
     * @param keys   non-null caller-owned keys
     * @param offset first key in the slice
     * @param length number of keys, possibly zero
     *
     * @throws IllegalArgumentException when the slice is outside the array
     */
    void putLongs(long[] keys, int offset, int length);

    /**
     * Compatibility bridge for generic callers. No deletion marker is allowed.
     *
     * @param key   non-null key
     * @param value exactly {@link NullValue#NULL}
     */
    @Override
    default void put(final Long key, final NullValue value) {
        final long primitiveKey = Vldtn.requireNonNull(key, "key").longValue();
        if (Vldtn.requireNonNull(value, "value") != NullValue.NULL) {
            throw new IllegalArgumentException(
                    "Long sets only accept NullValue.NULL.");
        }
        putLong(primitiveKey);
    }
}
