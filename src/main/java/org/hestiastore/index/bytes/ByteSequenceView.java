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
            EMPTY_ARRAY);

    private final byte[] data;

    /**
     * Creates a zero-copy view over {@code data}.
     *
     * @param data byte array backing the instance
     * @return {@link ByteSequenceView} referencing the full array
     */
    public static ByteSequenceView of(final byte[] data) {
        final byte[] validated = Vldtn.requireNonNull(data, "data");
        if (validated.length == 0) {
            return EMPTY_INSTANCE;
        }
        return new ByteSequenceView(validated);
    }

    private ByteSequenceView(final byte[] data) {
        this.data = data;
    }

    @Override
    public byte[] toByteArray() {
        if (data.length == 0) {
            return EMPTY_ARRAY;
        }
        return data;
    }

    @Override
    public int length() {
        return data.length;
    }

    @Override
    public byte getByte(final int index) {
        if (index < 0 || index >= data.length) {
            throw new IllegalArgumentException(String.format(
                    "Property 'index' must be between 0 and %d (inclusive). Got: %d",
                    Math.max(data.length - 1, 0), index));
        }
        return data[index];
    }

    @Override
    public ByteSequence slice(final int fromInclusive, final int toExclusive) {
        if (fromInclusive < 0 || toExclusive < fromInclusive
                || toExclusive > data.length) {
            throw new IllegalArgumentException(String.format(
                    "Slice range [%d, %d) exceeds sequence length %d",
                    fromInclusive, toExclusive, data.length));
        }
        final int sliceLength = toExclusive - fromInclusive;
        if (sliceLength == 0) {
            return ByteSequence.EMPTY;
        }
        return new ByteSequenceSlice(data, fromInclusive, sliceLength);
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
        return "Bytes{" + "length=" + data.length + '}';
    }
}
