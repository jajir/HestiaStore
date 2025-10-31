package org.hestiastore.index;

/**
 * Utility helpers for working with {@link Bytes} instances.
 */
public final class ByteTool {

    private ByteTool() {
        // utility class
    }

    /**
     * Counts how many leading bytes are identical in both buffers.
     *
     * @param first  the first Bytes instance
     * @param second the second Bytes instance
     * @return number of matching bytes from the beginning of the buffers
     */
    public static int countMatchingPrefixBytes(final ByteSequence first,
            final ByteSequence second) {
        Vldtn.requireNonNull(first, "first");
        Vldtn.requireNonNull(second, "second");
        final int limit = Math.min(first.length(), second.length());
        int index = 0;
        while (index < limit && first.getByte(index) == second.getByte(index)) {
            index++;
        }
        return index;
    }

    /**
     * Returns a slice of {@code full} starting at {@code index} wrapped in
     * Bytes.
     *
     * @param index start index (inclusive)
     * @param full  source Bytes instance
     * @return Bytes containing remaining bytes from {@code index}
     */
    public static ByteSequence getRemainingBytesAfterIndex(final int index,
            final ByteSequence full) {
        Vldtn.requireNonNull(full, "full");
        final int length = full.length();
        if (index < 0 || index > length) {
            throw new IllegalArgumentException(String
                    .format("Index '%d' is out of range 0..%d", index, length));
        }
        if (index == length) {
            return Bytes.EMPTY;
        }
        if (index == 0) {
            return full;
        }
        return full.slice(index, length);
    }

    /**
     * Concatenates two Bytes instances into a new Bytes instance.
     *
     * @param first  non-null first Bytes instance
     * @param second non-null second Bytes instance
     * @return new Bytes comprising both inputs
     */
    public static ByteSequence concatenate(final ByteSequence first,
            final ByteSequence second) {
        Vldtn.requireNonNull(first, "first");
        Vldtn.requireNonNull(second, "second");

        return ConcatenatedByteSequence.of(first, second);
    }
}
