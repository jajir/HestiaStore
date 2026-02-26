package org.hestiastore.index.datatype;

import java.util.Comparator;

/**
 * Defines full binary behavior of a value type used by the storage engine.
 *
 * <p>
 * A descriptor provides all components needed by index internals:
 * comparator, streaming reader/writer, and array-based decoder/encoder.
 * </p>
 *
 * @param <T> described value type
 */
public interface TypeDescriptor<T> {

    /**
     * Returns ordering logic for values of this type.
     *
     * @return comparator used in sorted index structures
     */
    Comparator<T> getComparator();

    /**
     * Returns stream-based reader for this type.
     *
     * @return type reader
     */
    TypeReader<T> getTypeReader();

    /**
     * Returns stream-based writer for this type.
     *
     * @return type writer
     */
    TypeWriter<T> getTypeWriter();

    /**
     * Returns array-based decoder for this type.
     *
     * @return type decoder
     */
    TypeDecoder<T> getTypeDecoder();

    /**
     * Returns array-based encoder for this type.
     *
     * @return type encoder
     */
    TypeEncoder<T> getTypeEncoder();

    /**
     * Returns the sentinel value used as a tombstone for delete semantics.
     *
     * @return tombstone value
     */
    T getTombstone();

    /**
     * Returns whether the supplied value equals the tombstone sentinel.
     *
     * @param value value to test
     * @return {@code true} when the value is a tombstone
     */
    default boolean isTombstone(final T value) {
        if (value == null) {
            return false;
        }
        return getTombstone().equals(value);
    }

}
