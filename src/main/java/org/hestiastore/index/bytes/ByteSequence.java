package org.hestiastore.index.bytes;

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
     * isolated copy is required, use {@link #toByteArray()} or
     * {@link ByteSequences#copyOf(byte[])} on the slice.
     *
     * @param fromInclusive start index (inclusive)
     * @param toExclusive   end index (exclusive)
     * @return a byte sequence representing the requested range
     */
    ByteSequence slice(int fromInclusive, int toExclusive);

    /**
     * Returns the bytes of this sequence as a new array.
     *
     * @return copy of the sequence data
     */
    byte[] toByteArray();

    /**
     * Indicates whether this sequence is empty.
     *
     * @return {@code true} when the sequence contains no bytes
     */
    default boolean isEmpty() {
        return length() == 0;
    }
}
