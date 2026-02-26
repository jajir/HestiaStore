package org.hestiastore.index.bytes;

import java.util.Arrays;

/**
 * Provides read-only access to a contiguous sequence of bytes.
 */
public interface ByteSequence {

    /**
     * Shared immutable empty sequence instance.
     */
    ByteSequence EMPTY = ByteSequenceEmpty.INSTANCE;

    /**
     * Returns the number of bytes contained in this sequence.
     *
     * @return length of the sequence
     */
    int length();

    /**
     * Returns the byte at the specified index.
     *
     * @param index zero-based index of the byte to return
     * @return byte value at the given index
     */
    byte getByte(int index);

    /**
     * Returns a slice of this sequence between the given offsets.
     * Implementations may return a lightweight view backed by the original
     * storage, so callers must not mutate the underlying data while retaining
     * references to the slice unless that sharing is intentional. If an
     * isolated copy is required, use {@link #toByteArrayCopy()}.
     *
     * @param fromInclusive start index (inclusive)
     * @param toExclusive   end index (exclusive)
     * @return a byte sequence representing the requested range
     */
    ByteSequence slice(int fromInclusive, int toExclusive);

    /**
     * Materialises this sequence as a byte array.
     * <p>
     * Implementations may return a shared backing or cached array to avoid
     * repeated allocations. Callers must treat the returned array as read-only.
     * Use {@link #toByteArrayCopy()} when an isolated mutable copy is required.
     * </p>
     *
     * @return byte array representation of this sequence
     */
    byte[] toByteArray();

    /**
     * Returns a defensive copy of this sequence content.
     *
     * @return new array containing the sequence bytes
     */
    default byte[] toByteArrayCopy() {
        return Arrays.copyOf(toByteArray(), length());
    }

    /**
     * Indicates whether this sequence is empty.
     *
     * @return {@code true} when the sequence contains no bytes
     */
    default boolean isEmpty() {
        return length() == 0;
    }
}
