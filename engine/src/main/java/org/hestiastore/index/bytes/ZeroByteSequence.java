package org.hestiastore.index.bytes;

import org.hestiastore.index.Vldtn;

/**
 * Lightweight immutable sequence that represents a configurable number of zero
 * bytes. Useful for padding without allocating new arrays for each request.
 */
@SuppressWarnings("java:S6206")
public final class ZeroByteSequence implements ByteSequence {

    private final int length;

    /**
     * Creates a zero-filled sequence of the requested length.
     *
     * @param length number of zeros to expose
     */
    public ZeroByteSequence(final int length) {
        this.length = Vldtn.requireGreaterThanOrEqualToZero(length, "length");
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
        return 0;
    }

    @Override
    public byte[] toByteArray() {
        if (length == 0) {
            return new byte[0];
        }
        return new byte[length];
    }

    @Override
    public ByteSequence slice(final int fromInclusive,
            final int toExclusive) {
        if (fromInclusive < 0) {
            throw new IllegalArgumentException(
                    "Property 'fromInclusive' must not be negative.");
        }
        if (toExclusive < fromInclusive) {
            throw new IllegalArgumentException(
                    "Property 'toExclusive' must not be smaller than 'fromInclusive'.");
        }
        if (toExclusive > length) {
            throw new IllegalArgumentException(String.format(
                    "Slice range [%d, %d) exceeds sequence length %d",
                    fromInclusive, toExclusive, length));
        }
        final int newLength = toExclusive - fromInclusive;
        if (newLength == 0) {
            return ByteSequence.EMPTY;
        }
        if (newLength == length) {
            return this;
        }
        return new ZeroByteSequence(newLength);
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
        return "ZeroByteSequence{" + "length=" + length + '}';
    }
}
