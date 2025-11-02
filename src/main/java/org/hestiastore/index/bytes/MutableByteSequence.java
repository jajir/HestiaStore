package org.hestiastore.index.bytes;

/**
 * Provides read and write access to a mutable sequence of bytes.
 */
public interface MutableByteSequence extends ByteSequence {

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
}
