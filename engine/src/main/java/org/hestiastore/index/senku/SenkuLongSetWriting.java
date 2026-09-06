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
