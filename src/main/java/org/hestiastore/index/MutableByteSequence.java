package org.hestiastore.index;

/**
 * Provides read and write access to a mutable sequence of bytes.
 */
public interface MutableByteSequence extends ByteSequence {

    /**
     * Allocates a zero-initialized mutable sequence of the requested size.
     *
     * @param size number of bytes to allocate (must be {@code >= 0})
     * @return new mutable byte sequence backed by a fresh array
     */
    static MutableByteSequence allocate(final int size) {
        return MutableBytes.allocate(size);
    }

    /**
     * Wraps the provided array without copying.
     *
     * @param array backing array to wrap (not copied)
     * @return mutable sequence backed by {@code array}
     */
    static MutableByteSequence wrap(final byte[] array) {
        return MutableBytes.wrap(array);
    }

    /**
     * Creates a mutable copy of the provided sequence.
     *
     * @param source sequence to copy
     * @return mutable sequence containing the same bytes as {@code source}
     */
    static MutableByteSequence copyOf(final ByteSequence source) {
        return MutableBytes.copyOf(source);
    }

    /**
     * Writes a single byte at the specified index.
     *
     * @param index zero-based index to write to
     * @param value byte value to set
     */
    void setByte(int index, byte value);

    /**
     * Copies bytes from the provided source sequence into this sequence.
     *
     * @param targetOffset index in this sequence to start writing to
     * @param source       input byte sequence
     * @param sourceOffset index in the source sequence to start copying from
     * @param length       number of bytes to copy
     */
    void setBytes(int targetOffset, ByteSequence source, int sourceOffset,
            int length);

    /**
     * Copies all bytes from the provided source sequence into this sequence
     * starting at the specified offset.
     *
     * @param targetOffset index in this sequence to start writing to
     * @param source       input byte sequence
     */
    default void setBytes(final int targetOffset, final ByteSequence source) {
        setBytes(targetOffset, source, 0, source.length());
    }

    /**
     * Returns an immutable copy of this sequence as {@link Bytes}.
     *
     * @return immutable bytes containing the same data
     */
    default Bytes toBytes() {
        return Bytes.copyOf(this);
    }
}
