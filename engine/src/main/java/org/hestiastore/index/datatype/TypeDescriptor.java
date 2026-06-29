package org.hestiastore.index.datatype;

import java.util.Comparator;
import java.util.OptionalInt;

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
     * Returns an estimated average serialized size in bytes for this type.
     *
     * <p>
     * Fixed-size descriptors should return their constant serialized size.
     * Variable-size descriptors should return a conservative estimate only when
     * the descriptor can make one safely. An empty value means that memory
     * estimation cannot model this type without a custom descriptor or another
     * explicit source of size information.
     * </p>
     * <p>
     * Empty is different from zero. {@link OptionalInt#empty()} means that the
     * descriptor does not know a defensible estimate. {@code OptionalInt.of(0)}
     * is a real estimate for descriptors that serialize no bytes, such as a
     * null-value marker. Estimation code should treat unknown estimates
     * explicitly instead of silently guessing.
     * </p>
     *
     * @return estimated average serialized size, or empty when unknown
     */
    default OptionalInt getEstimatedAverageSizeInBytes() {
        return OptionalInt.empty();
    }

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
