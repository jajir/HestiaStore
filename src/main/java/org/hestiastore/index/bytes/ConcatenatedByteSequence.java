package org.hestiastore.index.bytes;

import org.hestiastore.index.Vldtn;

/**
 * Lightweight {@link ByteSequence} implementation that presents two underlying
 * sequences as a single contiguous view without copying.
 */
public final class ConcatenatedByteSequence extends ByteSequenceCaching {

    private final ByteSequence first;
    private final ByteSequence second;
    private final int firstLength;
    private final int totalLength;

    private ConcatenatedByteSequence(final ByteSequence first,
            final ByteSequence second) {
        this.first = first;
        this.second = second;
        this.firstLength = first.length();
        this.totalLength = Math.addExact(firstLength, second.length());
    }

    /**
     * Creates a concatenated view over the provided sequences.
     *
     * @param first  non-null first sequence
     * @param second non-null second sequence
     * @return {@link ByteSequence} representing the concatenation
     */
    public static ByteSequence of(final ByteSequence first,
            final ByteSequence second) {
        final ByteSequence validatedFirst = Vldtn.requireNonNull(first,
                "first");
        final ByteSequence validatedSecond = Vldtn.requireNonNull(second,
                "second");
        if (validatedFirst.isEmpty()) {
            return validatedSecond;
        }
        if (validatedSecond.isEmpty()) {
            return validatedFirst;
        }
        return new ConcatenatedByteSequence(validatedFirst, validatedSecond);
    }

    @Override
    public int length() {
        return totalLength;
    }

    @Override
    public byte getByte(final int index) {
        validateIndex(index);
        if (index < firstLength) {
            return first.getByte(index);
        }
        return second.getByte(index - firstLength);
    }

    @Override
    protected byte[] computeByteArray() {
        final byte[] firstBytes = first.toByteArray();
        final byte[] secondBytes = second.toByteArray();
        if (firstBytes.length == 0) {
            return secondBytes;
        }
        if (secondBytes.length == 0) {
            return firstBytes;
        }
        final byte[] combined = new byte[firstBytes.length
                + secondBytes.length];
        System.arraycopy(firstBytes, 0, combined, 0, firstBytes.length);
        System.arraycopy(secondBytes, 0, combined, firstBytes.length,
                secondBytes.length);
        return combined;
    }

    @Override
    public ByteSequence slice(final int fromInclusive, final int toExclusive) {
        validateSliceRange(fromInclusive, toExclusive, totalLength);
        if (fromInclusive == toExclusive) {
            return ByteSequence.EMPTY;
        }
        if (toExclusive <= firstLength) {
            return first.slice(fromInclusive, toExclusive);
        }
        if (fromInclusive >= firstLength) {
            return second.slice(fromInclusive - firstLength,
                    toExclusive - firstLength);
        }
        final ByteSequence firstPart = first.slice(fromInclusive, firstLength);
        final ByteSequence secondPart = second.slice(0,
                toExclusive - firstLength);
        return ConcatenatedByteSequence.of(firstPart, secondPart);
    }

    private void validateIndex(final int index) {
        if (index < 0 || index >= totalLength) {
            throw new IllegalArgumentException(String.format(
                    "Property 'index' must be between 0 and %d (inclusive). Got: %d",
                    Math.max(totalLength - 1, 0), index));
        }
    }

    private static void validateSliceRange(final int fromInclusive,
            final int toExclusive, final int capacity) {
        if (fromInclusive < 0) {
            throw new IllegalArgumentException(
                    "Property 'fromInclusive' must not be negative.");
        }
        if (toExclusive < fromInclusive) {
            throw new IllegalArgumentException(
                    "Property 'toExclusive' must not be smaller than 'fromInclusive'.");
        }
        if (toExclusive > capacity) {
            throw new IllegalArgumentException(String.format(
                    "Property 'toExclusive' with capacity %d exceeds sequence length %d",
                    toExclusive, capacity));
        }
    }

}
