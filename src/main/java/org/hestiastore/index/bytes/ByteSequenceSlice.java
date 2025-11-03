package org.hestiastore.index.bytes;

import java.util.Arrays;

/**
 * Lightweight slice view backed by a byte array. Stores an offset and length
 * referencing the underlying immutable array without copying.
 */
public final class ByteSequenceSlice extends ByteSequenceCaching {

    private final byte[] data;
    private final int offset;
    private final int length;

    ByteSequenceSlice(final byte[] data, final int absoluteOffset,
            final int length) {
        this.data = data;
        this.offset = absoluteOffset;
        this.length = length;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public byte getByte(final int index) {
        if (index < 0 || index >= length) {
            throw new IllegalArgumentException(String.format(
                    "Property 'index' must be between 0 and %d (inclusive). Got: %d",
                    Math.max(length - 1, 0), index));
        }
        return data[offset + index];
    }

    @Override
    public ByteSequence slice(final int fromInclusive,
            final int toExclusive) {
        if (fromInclusive < 0 || toExclusive < fromInclusive
                || toExclusive > length) {
            throw new IllegalArgumentException(String.format(
                    "Slice range [%d, %d) exceeds sequence length %d",
                    fromInclusive, toExclusive, length));
        }
        final int newLength = toExclusive - fromInclusive;
        if (newLength == 0) {
            return ByteSequence.EMPTY;
        }
        return new ByteSequenceSlice(data, offset + fromInclusive, newLength);
    }

    @Override
    protected byte[] computeByteArray() {
        if (length == 0) {
            return new byte[0];
        }
        return Arrays.copyOfRange(data, offset, offset + length);
    }

    @Override
    public int hashCode() {
        return ByteSequences.contentHashCode(this);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ByteSequence)) {
            return false;
        }
        return ByteSequences.contentEquals(this, (ByteSequence) obj);
    }

    @Override
    public String toString() {
        return "ByteSequenceSlice{" + "offset=" + offset + ", length=" + length
                + '}';
    }
}
