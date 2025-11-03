package org.hestiastore.index.bytes;

import org.hestiastore.index.Vldtn;

/**
 * Mutable byte buffer implementation backed by a byte array.
 */
public final class MutableBytes implements MutableByteSequence {

    private final byte[] data;

    private MutableBytes(final byte[] data) {
        this.data = Vldtn.requireNonNull(data, "data");
    }

    /**
     * Allocates a new buffer of the requested size.
     *
     * @param size number of bytes to allocate (must be {@code >= 0})
     * @return new mutable buffer initialized with zeros
     */
    public static MutableBytes allocate(final int size) {
        if (size < 0) {
            throw new IllegalArgumentException(
                    "Property 'size' must be greater than or equal to 0");
        }
        return new MutableBytes(new byte[size]);
    }

    /**
     * Wraps the provided array without copying.
     *
     * @param array backing array (not copied)
     * @return new mutable buffer backed by {@code array}
     */
    public static MutableBytes wrap(final byte[] array) {
        return new MutableBytes(array);
    }

    /**
     * Creates a mutable copy of the provided sequence.
     *
     * @param sequence source sequence
     * @return mutable copy containing the same bytes
     */
    public static MutableBytes copyOf(final ByteSequence sequence) {
        Vldtn.requireNonNull(sequence, "sequence");
        final MutableBytes copy = allocate(sequence.length());
        copy.setBytes(0, sequence);
        return copy;
    }

    @Override
    public int length() {
        return data.length;
    }

    /**
     * Returns the backing array. Intended for efficient interop with IO
     * components that can fill or consume the full buffer.
     *
     * @return backing array containing the mutable bytes
     */
    public byte[] array() {
        return data;
    }

    @Override
    public byte getByte(final int index) {
        validateIndex(index);
        return data[index];
    }

    @Override
    public void copyTo(final int sourceOffset, final byte[] target,
            final int targetOffset, final int length) {
        Vldtn.requireNonNull(target, "target");
        validateRange(sourceOffset, length, data.length, "sourceOffset");
        validateRange(targetOffset, length, target.length, "targetOffset");
        if (length == 0) {
            return;
        }
        System.arraycopy(data, sourceOffset, target, targetOffset, length);
    }

    @Override
    public byte[] toByteArray() {
        final byte[] copy = new byte[length()];
        copyTo(0, copy, 0, copy.length);
        return copy;
    }

    @Override
    public ByteSequence slice(final int fromInclusive, final int toExclusive) {
        validateRange(fromInclusive, toExclusive - fromInclusive, data.length,
                "fromInclusive");
        final int sliceLength = toExclusive - fromInclusive;
        if (sliceLength == 0) {
            return ByteSequence.EMPTY;
        }
        return ByteSequenceView.of(data, fromInclusive, sliceLength);
    }

    @Override
    public void setByte(final int index, final byte value) {
        validateIndex(index);
        data[index] = value;
    }

    @Override
    public void setBytes(final int targetOffset, final ByteSequence source,
            final int sourceOffset, final int length) {
        Vldtn.requireNonNull(source, "source");
        validateRange(targetOffset, length, data.length, "targetOffset");
        source.copyTo(sourceOffset, data, targetOffset, length);
    }

    /**
     * Returns a lightweight {@link ByteSequence} view over the current buffer
     * without copying the underlying array.
     *
     * @return byte sequence backed directly by this buffer's array
     */
    public ByteSequence toByteSequence() {
        if (data.length == 0) {
            return ByteSequence.EMPTY;
        }
        return ByteSequenceView.of(data, 0, data.length);
    }

    /**
     * Returns an immutable {@link ByteSequenceView} view over the current
     * buffer without copying. Callers must not mutate the returned bytes.
     *
     * @return {@link ByteSequenceView} backed by this buffer's array
     */
    public ByteSequence toImmutableBytes() {
        if (data.length == 0) {
            return ByteSequence.EMPTY;
        }
        return ByteSequenceView.of(data, 0, data.length);
    }

    private void validateIndex(final int index) {
        if (index < 0 || index >= data.length) {
            throw new IllegalArgumentException(String.format(
                    "Property 'index' must be between 0 and %d (inclusive). Got: %d",
                    Math.max(data.length - 1, 0), index));
        }
    }

    private static void validateRange(final int offset, final int length,
            final int capacity, final String propertyName) {
        if (offset < 0) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' must not be negative.", propertyName));
        }
        if (length < 0) {
            throw new IllegalArgumentException(
                    "Property 'length' must not be negative.");
        }
        if (offset > capacity || offset + length > capacity) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' with length %d exceeds capacity %d",
                    propertyName, length, capacity));
        }
    }
}
