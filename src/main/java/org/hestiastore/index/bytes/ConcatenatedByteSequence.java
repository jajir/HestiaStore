package org.hestiastore.index.bytes;

import org.hestiastore.index.Vldtn;

/**
 * Lightweight {@link ByteSequence} implementation that presents two underlying
 * sequences as a single contiguous view without copying.
 */
public final class ConcatenatedByteSequence implements ByteSequence {

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
    public void copyTo(final int sourceOffset, final byte[] target,
            final int targetOffset, final int length) {
        Vldtn.requireNonNull(target, "target");
        validateRange(sourceOffset, length, totalLength, "sourceOffset");
        validateRange(targetOffset, length, target.length, "targetOffset");
        if (length == 0) {
            return;
        }
        int remaining = length;
        int currentSourceOffset = sourceOffset;
        int currentTargetOffset = targetOffset;
        if (currentSourceOffset < firstLength) {
            final int firstAvailable = Math
                    .min(firstLength - currentSourceOffset, remaining);
            first.copyTo(currentSourceOffset, target, currentTargetOffset,
                    firstAvailable);
            currentSourceOffset += firstAvailable;
            currentTargetOffset += firstAvailable;
            remaining -= firstAvailable;
        }
        if (remaining > 0) {
            final int secondOffset = currentSourceOffset - firstLength;
            second.copyTo(secondOffset, target, currentTargetOffset, remaining);
        }
    }

    @Override
    public byte[] toByteArray() {
        final byte[] copy = new byte[length()];
        copyTo(0, copy, 0, copy.length);
        return copy;
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
