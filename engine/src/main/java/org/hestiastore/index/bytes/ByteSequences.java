package org.hestiastore.index.bytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        return ByteSequenceView.of(validated);
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
        if (fromInclusive == 0 && toExclusive == validated.length) {
            return ByteSequenceView.of(validated);
        }
        return new ByteSequenceSlice(validated, fromInclusive, length);
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
        return ByteSequenceView.of(copy);
    }

    /**
     * Creates an immutable {@link ByteSequence} from the provided sequence.
     * <p>
     * This method always returns an isolated defensive copy for non-empty
     * sequences.
     * </p>
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
        final byte[] copy = new byte[validated.length()];
        copy(validated, 0, copy, 0, copy.length);
        return ByteSequenceView.of(copy);
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
        copy(validated, 0, data, 0, currentLength);
        return ByteSequenceView.of(data);
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
     * Concatenates sequences into a single logical sequence without copying
     * payload bytes.
     *
     * @param sequences input sequences in order
     * @return concatenated sequence, or {@link ByteSequence#EMPTY} when all
     *         inputs are empty
     */
    public static ByteSequence concat(
            final Iterable<? extends ByteSequence> sequences) {
        final Iterable<? extends ByteSequence> validated = Vldtn
                .requireNonNull(sequences, "sequences");
        final List<ByteSequence> parts = new ArrayList<>();
        for (ByteSequence sequence : validated) {
            final ByteSequence validatedPart = Vldtn.requireNonNull(sequence,
                    "sequence");
            if (!validatedPart.isEmpty()) {
                parts.add(validatedPart);
            }
        }
        if (parts.isEmpty()) {
            return ByteSequence.EMPTY;
        }
        return concatNonEmpty(parts);
    }

    /**
     * Concatenates a non-empty list of non-empty sequences without additional
     * filtering.
     *
     * @param sequences non-empty list of non-empty sequences
     * @return concatenated sequence
     */
    public static ByteSequence concatNonEmpty(
            final List<? extends ByteSequence> sequences) {
        final List<? extends ByteSequence> validated = Vldtn
                .requireNonNull(sequences, "sequences");
        if (validated.isEmpty()) {
            throw new IllegalArgumentException(
                    "Property 'sequences' must not be empty.");
        }
        if (validated.size() == 1) {
            final ByteSequence only = Vldtn.requireNonNull(validated.get(0),
                    "sequence");
            if (only.isEmpty()) {
                throw new IllegalArgumentException(
                        "Property 'sequences' must not contain empty sequence.");
            }
            return only;
        }
        for (ByteSequence sequence : validated) {
            final ByteSequence part = Vldtn.requireNonNull(sequence,
                    "sequence");
            if (part.isEmpty()) {
                throw new IllegalArgumentException(
                        "Property 'sequences' must not contain empty sequence.");
            }
        }
        return concatBalanced(validated, 0, validated.size());
    }

    private static ByteSequence concatBalanced(
            final List<? extends ByteSequence> parts,
            final int fromInclusive, final int toExclusive) {
        final int size = toExclusive - fromInclusive;
        if (size == 1) {
            return parts.get(fromInclusive);
        }
        if (size == 2) {
            return ConcatenatedByteSequence.of(parts.get(fromInclusive),
                    parts.get(fromInclusive + 1));
        }
        final int mid = fromInclusive + (size / 2);
        return ConcatenatedByteSequence.of(
                concatBalanced(parts, fromInclusive, mid),
                concatBalanced(parts, mid, toExclusive));
    }

    /**
     * Copies bytes from {@code source} into {@code target}.
     */
    public static void copy(final ByteSequence source, final int sourceOffset,
            final byte[] target, final int targetOffset, final int length) {
        final ByteSequence validatedSource = Vldtn.requireNonNull(source,
                "source");
        final byte[] validatedTarget = Vldtn.requireNonNull(target, "target");
        validateCopyRange(sourceOffset, length, validatedSource.length(),
                "sourceOffset");
        validateCopyRange(targetOffset, length, validatedTarget.length,
                "targetOffset");
        if (length == 0) {
            return;
        }
        if (validatedSource instanceof ByteSequenceView) {
            ((ByteSequenceView) validatedSource).copyTo(sourceOffset,
                    validatedTarget, targetOffset, length);
            return;
        }
        if (validatedSource instanceof ByteSequenceSlice) {
            ((ByteSequenceSlice) validatedSource).copyTo(sourceOffset,
                    validatedTarget, targetOffset, length);
            return;
        }
        if (validatedSource instanceof MutableBytes) {
            ((MutableBytes) validatedSource).copyTo(sourceOffset, validatedTarget,
                    targetOffset, length);
            return;
        }
        for (int index = 0; index < length; index++) {
            validatedTarget[targetOffset + index] = validatedSource
                    .getByte(sourceOffset + index);
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
        if (offset > capacity) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' with length %d exceeds capacity %d",
                    propertyName, len, capacity));
        }
        final long end = (long) offset + (long) len;
        if (end > capacity) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' with length %d exceeds capacity %d",
                    propertyName, len, capacity));
        }
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
