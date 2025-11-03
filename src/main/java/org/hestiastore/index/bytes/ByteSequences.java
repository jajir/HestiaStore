package org.hestiastore.index.bytes;

import java.util.Arrays;

import org.hestiastore.index.Vldtn;

/**
 * Factory and utility helpers for working with {@link ByteSequence} instances.
 */
public final class ByteSequences {

    private ByteSequences() {
        // utility class
    }

    /**
     * Wraps the provided array without copying it.
     *
     * @param array backing array (not copied)
     * @return {@link ByteSequence} view over {@code array}
     */
    public static ByteSequence wrap(final byte[] array) {
        final byte[] validated = Vldtn.requireNonNull(array, "array");
        if (validated.length == 0) {
            return ByteSequence.EMPTY;
        }
        return ByteSequenceView.of(validated, 0, validated.length);
    }

    /**
     * Creates a zero-copy view over the specified range of {@code array}.
     *
     * @param array         backing array
     * @param fromInclusive start index (inclusive)
     * @param toExclusive   end index (exclusive)
     * @return {@link ByteSequence} representing the requested range
     */
    public static ByteSequence viewOf(final byte[] array,
            final int fromInclusive, final int toExclusive) {
        final byte[] validated = Vldtn.requireNonNull(array, "array");
        if (fromInclusive < 0 || toExclusive < fromInclusive
                || toExclusive > validated.length) {
            throw new IllegalArgumentException(String.format(
                    "Slice range [%d, %d) exceeds array length %d",
                    fromInclusive, toExclusive, validated.length));
        }
        final int length = toExclusive - fromInclusive;
        if (length == 0) {
            return ByteSequence.EMPTY;
        }
        return ByteSequenceView.of(validated, fromInclusive, length);
    }

    /**
     * Creates an immutable copy of the provided array.
     *
     * @param array source array (not mutated)
     * @return {@link ByteSequence} backed by a defensive copy
     */
    public static ByteSequence copyOf(final byte[] array) {
        final byte[] validated = Vldtn.requireNonNull(array, "array");
        if (validated.length == 0) {
            return ByteSequence.EMPTY;
        }
        final byte[] copy = Arrays.copyOf(validated, validated.length);
        return ByteSequenceView.of(copy, 0, copy.length);
    }

    /**
     * Creates an immutable {@link ByteSequence} from the provided sequence,
     * reusing the backing storage when safe.
     *
     * @param sequence source sequence
     * @return immutable {@link ByteSequence} containing the sequence data
     */
    public static ByteSequence copyOf(final ByteSequence sequence) {
        final ByteSequence validated = Vldtn.requireNonNull(sequence,
                "sequence");
        if (validated.isEmpty()) {
            return ByteSequence.EMPTY;
        }
        if (validated instanceof ByteSequenceView) {
            return validated;
        }
        if (validated instanceof MutableBytes) {
            return ((MutableBytes) validated).toImmutableBytes();
        }
        final byte[] copy = new byte[validated.length()];
        validated.copyTo(0, copy, 0, copy.length);
        return ByteSequenceView.of(copy, 0, copy.length);
    }

    /**
     * Allocates a zero-filled mutable buffer of the requested size.
     *
     * @param size number of bytes to allocate (must be {@code >= 0})
     * @return mutable {@link ByteSequence} initialized with zeros
     */
    public static ByteSequence allocate(final int size) {
        if (size < 0) {
            throw new IllegalArgumentException(
                    "Property 'size' must be greater than or equal to 0");
        }
        return MutableBytes.allocate(size);
    }

    /**
     * Returns a sequence padded with zeros up to {@code targetLength}. Returns
     * the original sequence when it already meets or exceeds the requested
     * length.
     *
     * @param sequence     source sequence
     * @param targetLength desired minimum length (must be {@code >= 0})
     * @return {@link ByteSequence} with length {@code >= targetLength}
     */
    public static ByteSequence padToLength(final ByteSequence sequence,
            final int targetLength) {
        final ByteSequence validated = Vldtn.requireNonNull(sequence,
                "sequence");
        if (targetLength < 0) {
            throw new IllegalArgumentException(
                    "Property 'targetLength' must not be negative.");
        }
        final int currentLength = validated.length();
        if (currentLength >= targetLength) {
            return validated;
        }
        final byte[] data = new byte[targetLength];
        validated.copyTo(0, data, 0, currentLength);
        return ByteSequenceView.of(data, 0, data.length);
    }

    /**
     * Pads the provided sequence with zeros to align its length to the next
     * multiple of {@code cellSize}.
     *
     * @param sequence source sequence
     * @param cellSize positive cell size to pad to
     * @return {@link ByteSequence} aligned to the requested cell size
     */
    public static ByteSequence padToCell(final ByteSequence sequence,
            final int cellSize) {
        final ByteSequence validated = Vldtn.requireNonNull(sequence,
                "sequence");
        Vldtn.requireGreaterThanZero(cellSize, "cellSize");
        final int currentLength = validated.length();
        if (currentLength == 0) {
            return validated;
        }
        final int remainder = currentLength % cellSize;
        if (remainder == 0) {
            return validated;
        }
        final int paddedLength = Math.addExact(currentLength,
                cellSize - remainder);
        return padToLength(validated, paddedLength);
    }

    /**
     * Performs a byte-wise equality comparison between two sequences.
     *
     * @param first  first sequence
     * @param second second sequence
     * @return {@code true} when the sequences have identical content
     */
    public static boolean contentEquals(final ByteSequence first,
            final ByteSequence second) {
        if (first == second) {
            return true;
        }
        final ByteSequence validatedFirst = Vldtn.requireNonNull(first,
                "first");
        final ByteSequence validatedSecond = Vldtn.requireNonNull(second,
                "second");
        final int length = validatedFirst.length();
        if (length != validatedSecond.length()) {
            return false;
        }
        for (int index = 0; index < length; index++) {
            if (validatedFirst.getByte(index) != validatedSecond
                    .getByte(index)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Computes a content-based hash code compatible with
     * {@link java.util.Arrays#hashCode(byte[])}.
     *
     * @param sequence source sequence
     * @return hash code derived from the sequence content
     */
    public static int contentHashCode(final ByteSequence sequence) {
        final ByteSequence validated = Vldtn.requireNonNull(sequence,
                "sequence");
        int result = 1;
        for (int index = 0; index < validated.length(); index++) {
            result = 31 * result + validated.getByte(index);
        }
        return result;
    }
}
