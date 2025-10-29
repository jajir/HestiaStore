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
    public static Bytes getRemainingBytesAfterIndex(final int index,
            final ByteSequence full) {
        Vldtn.requireNonNull(full, "full");
        if (index < 0 || index > full.length()) {
            throw new IllegalArgumentException(String.format(
                    "Index '%d' is out of range 0..%d", index, full.length()));
        }
        final int remainingLength = full.length() - index;
        if (remainingLength == 0) {
            return Bytes.EMPTY;
        }
        final MutableBytes slice = MutableBytes.allocate(remainingLength);
        slice.setBytes(0, full, index, remainingLength);
        return slice.toBytes();
    }

    /**
     * Concatenates two Bytes instances into a new Bytes instance.
     *
     * @param first  non-null first Bytes instance
     * @param second non-null second Bytes instance
     * @return new Bytes comprising both inputs
     */
    public static Bytes concatenate(final ByteSequence first,
            final ByteSequence second) {
        Vldtn.requireNonNull(first, "first");
        Vldtn.requireNonNull(second, "second");

        final MutableBytes out = MutableBytes
                .allocate(first.length() + second.length());
        out.setBytes(0, first);
        out.setBytes(first.length(), second);
        return out.toBytes();
    }
}
