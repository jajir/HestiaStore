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
    public static int countMatchingPrefixBytes(final Bytes first,
            final Bytes second) {
        Vldtn.requireNonNull(first, "first");
        Vldtn.requireNonNull(second, "second");
        // FIXME should avoid this just takes bytes from source
        final byte[] firstData = first.getData();
        final byte[] secondData = second.getData();
        final int limit = Math.min(first.length(), second.length());
        int index = 0;
        while (index < limit && firstData[index] == secondData[index]) {
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
            final Bytes full) {
        Vldtn.requireNonNull(full, "full");
        if (index < 0 || index > full.length()) {
            throw new IllegalArgumentException(String.format(
                    "Index '%d' is out of range 0..%d", index, full.length()));
        }
        final int remainingLength = full.length() - index;
        final Bytes slice = Bytes.allocate(remainingLength);
        System.arraycopy(full.getData(), index, slice.getData(), 0,
                remainingLength);
        return slice;
    }

    /**
     * Concatenates two Bytes instances into a new Bytes instance.
     *
     * @param first  non-null first Bytes instance
     * @param second non-null second Bytes instance
     * @return new Bytes comprising both inputs
     */
    public static Bytes concatenate(final Bytes first, final Bytes second) {
        Vldtn.requireNonNull(first, "first");
        Vldtn.requireNonNull(second, "second");

        final Bytes out = Bytes.allocate(first.length() + second.length());
        System.arraycopy(first.getData(), 0, out.getData(), 0, first.length());
        System.arraycopy(second.getData(), 0, out.getData(), first.length(),
                second.length());
        return out;
    }
}
