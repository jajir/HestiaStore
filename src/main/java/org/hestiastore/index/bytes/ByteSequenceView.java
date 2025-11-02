package org.hestiastore.index.bytes;

import org.hestiastore.index.Vldtn;

/**
 * Lightweight {@link ByteSequence} implementation that shares the backing array
 * with another sequence using an offset and length.
 */
public final class ByteSequenceView implements ByteSequence {

    private final byte[] data;
    private final int offset;
    private final int length;

    private ByteSequenceView(final byte[] data, final int offset,
            final int length) {
        this.data = Vldtn.requireNonNull(data, "data");
        this.offset = offset;
        this.length = length;
    }

    /**
     * Creates a new view over the provided {@code data} array.
     *
     * @param data          backing array
     * @param fromInclusive start index (inclusive)
     * @param toExclusive   end index (exclusive)
     * @return byte sequence view spanning the requested range
     */
    public static ByteSequence of(final byte[] data, final int fromInclusive,
            final int toExclusive) {
        validateRange(data, fromInclusive, toExclusive);
        final int viewLength = toExclusive - fromInclusive;
        if (viewLength == 0) {
            return Bytes.EMPTY;
        }
        return new ByteSequenceView(data, fromInclusive, viewLength);
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public byte getByte(final int index) {
        validateIndex(index);
        return data[offset + index];
    }

    @Override
    public void copyTo(final int sourceOffset, final byte[] target,
            final int targetOffset, final int len) {
        Vldtn.requireNonNull(target, "target");
        validateCopyRange(sourceOffset, len, length, "sourceOffset");
        validateCopyRange(targetOffset, len, target.length, "targetOffset");
        if (len == 0) {
            return;
        }
        System.arraycopy(data, offset + sourceOffset, target, targetOffset,
                len);
    }

    @Override
    public ByteSequence slice(final int fromInclusive, final int toExclusive) {
        validateCopyRange(fromInclusive, toExclusive - fromInclusive, length,
                "fromInclusive");
        final int absoluteFrom = offset + fromInclusive;
        final int absoluteTo = absoluteFrom + (toExclusive - fromInclusive);
        return ByteSequenceView.of(data, absoluteFrom, absoluteTo);
    }

    private void validateIndex(final int index) {
        validateCopyRange(index, 1, length, "index");
    }

    private static void validateRange(final byte[] data,
            final int fromInclusive, final int toExclusive) {
        Vldtn.requireNonNull(data, "data");
        if (fromInclusive < 0 || toExclusive < fromInclusive
                || toExclusive > data.length) {
            throw new IllegalArgumentException(
                    String.format("Slice range [%d, %d) exceeds capacity %d",
                            fromInclusive, toExclusive, data.length));
        }
    }

    private static void validateCopyRange(final int offset, final int len,
            final int capacity, final String propertyName) {
        if (offset < 0) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' must not be negative.", propertyName));
        }
        if (len < 0) {
            throw new IllegalArgumentException(
                    "Property 'length' must not be negative.");
        }
        if (offset > capacity || offset + len > capacity) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' with length %d exceeds capacity %d",
                    propertyName, len, capacity));
        }
    }
}
