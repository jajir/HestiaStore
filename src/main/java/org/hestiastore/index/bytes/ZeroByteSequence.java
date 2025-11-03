package org.hestiastore.index.bytes;

import java.util.Arrays;

/**
 * Lightweight immutable sequence that represents a configurable number of zero
 * bytes. Useful for padding without allocating new arrays for each request.
 */
public final class ZeroByteSequence implements ByteSequence {

    private final int length;

    /**
     * Creates a zero-filled sequence of the requested length.
     *
     * @param length number of zeros to expose
     */
    public ZeroByteSequence(final int length) {
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
        return 0;
    }

    @Override
    public byte[] toByteArray() {
        if (length == 0) {
            return new byte[0];
        }
        final byte[] copy = new byte[length];
        Arrays.fill(copy, (byte) 0);
        return copy;
    }

    @Override
    public ByteSequence slice(final int fromInclusive,
            final int toExclusive) {
        if (fromInclusive < 0 || toExclusive < fromInclusive
                || toExclusive > length) {
            throw new IllegalArgumentException(
                    "Invalid slice range for zero padding");
        }
        final int newLength = toExclusive - fromInclusive;
        if (newLength == 0) {
            return ByteSequence.EMPTY;
        }
        return new ZeroByteSequence(newLength);
    }
}
