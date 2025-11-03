package org.hestiastore.index.bytes;

import java.util.Arrays;

import org.hestiastore.index.Vldtn;

/**
 * Immutable {@link ByteSequence} backed by a byte array. Supports lightweight
 * slicing without copying by keeping offset and length metadata.
 */
public final class ByteSequenceView implements ByteSequence {

    private static final byte[] EMPTY_ARRAY = new byte[0];
    private static final ByteSequenceView EMPTY_INSTANCE = new ByteSequenceView(
            EMPTY_ARRAY, 0, 0);

    private final byte[] data;
    private final int offset;
    private final int length;

    /**
     * Creates a zero-copy view over {@code data}.
     *
     * @param data byte array backing the instance
     * @return {@link ByteSequenceView} referencing the full array
     */
    public static ByteSequenceView of(final byte[] data) {
        return of(Vldtn.requireNonNull(data, "data"), 0, data.length);
    }

    /**
     * Creates a view over a portion of {@code data} without copying.
     *
     * @param data   backing array
     * @param offset first byte index (inclusive)
     * @param length number of bytes in the view
     * @return {@link ByteSequenceView} view
     */
    static ByteSequenceView of(final byte[] data, final int offset,
            final int length) {
        final byte[] validated = Vldtn.requireNonNull(data, "data");
        if (length == 0) {
            return EMPTY_INSTANCE;
        }
        if (offset < 0 || length < 0 || offset > validated.length
                || offset + length > validated.length) {
            throw new IllegalArgumentException(String.format(
                    "Requested view [%d, %d) exceeds array length %d", offset,
                    offset + length, validated.length));
        }
        if (offset == 0 && length == validated.length) {
            return validated.length == 0 ? EMPTY_INSTANCE
                    : new ByteSequenceView(validated, 0, validated.length);
        }
        return new ByteSequenceView(validated, offset, length);
    }

    private ByteSequenceView(final byte[] data, final int offset,
            final int length) {
        this.data = data;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public byte[] toByteArray() {
        if (length == 0) {
            return EMPTY_ARRAY;
        }
        if (offset == 0 && length == data.length) {
            return data;
        }
        return Arrays.copyOfRange(data, offset, offset + length);
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
    public void copyTo(final int sourceOffset, final byte[] target,
            final int targetOffset, final int requestedLength) {
        Vldtn.requireNonNull(target, "target");
        if (sourceOffset < 0 || requestedLength < 0 || sourceOffset > length
                || sourceOffset + requestedLength > length) {
            throw new IllegalArgumentException(String.format(
                    "Property 'sourceOffset' with length %d exceeds capacity %d",
                    requestedLength, length));
        }
        if (targetOffset < 0 || targetOffset > target.length
                || targetOffset + requestedLength > target.length) {
            throw new IllegalArgumentException(String.format(
                    "Property 'targetOffset' with length %d exceeds capacity %d",
                    requestedLength, target.length));
        }
        if (requestedLength == 0) {
            return;
        }
        System.arraycopy(data, offset + sourceOffset, target, targetOffset,
                requestedLength);
    }

    @Override
    public ByteSequence slice(final int fromInclusive, final int toExclusive) {
        if (fromInclusive < 0 || toExclusive < fromInclusive
                || toExclusive > length) {
            throw new IllegalArgumentException(String.format(
                    "Slice range [%d, %d) exceeds sequence length %d",
                    fromInclusive, toExclusive, length));
        }
        final int sliceLength = toExclusive - fromInclusive;
        if (sliceLength == 0) {
            return ByteSequence.EMPTY;
        }
        return new ByteSequenceView(data, offset + fromInclusive, sliceLength);
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
        return "Bytes{" + "offset=" + offset + ", length=" + length + '}';
    }
}
