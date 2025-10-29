package org.hestiastore.index;

/**
 * Provides read-only access to a contiguous sequence of bytes.
 */
public interface ByteSequence {

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
     * Copies bytes from this sequence into the provided target array.
     *
     * @param sourceOffset index in this sequence to start copying from
     * @param target       destination byte array
     * @param targetOffset index in the destination array to start writing to
     * @param length       number of bytes to copy
     */
    void copyTo(int sourceOffset, byte[] target, int targetOffset, int length);

    /**
     * Returns a slice of this sequence between the given offsets.
     *
     * @param fromInclusive start index (inclusive)
     * @param toExclusive   end index (exclusive)
     * @return a new byte sequence containing the requested range
     */
    ByteSequence slice(int fromInclusive, int toExclusive);

    /**
     * Copies the entire sequence into the provided destination array.
     *
     * @param target       destination byte array
     * @param targetOffset index in the destination array to start writing to
     */
    default void copyTo(final byte[] target, final int targetOffset) {
        copyTo(0, target, targetOffset, length());
    }

    /**
     * Returns the bytes of this sequence as a new array.
     *
     * @return copy of the sequence data
     */
    default byte[] toByteArray() {
        final byte[] copy = new byte[length()];
        copyTo(0, copy, 0, copy.length);
        return copy;
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
