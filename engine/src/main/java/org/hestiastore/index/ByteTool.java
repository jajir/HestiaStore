package org.hestiastore.index;

/**
 * Utility helpers for working with byte arrays.
 *
 * @deprecated Prefer {@link org.hestiastore.index.bytes.ByteTool} for indexed
 *             byte access, or JDK array helpers for direct byte-array work.
 */
@Deprecated(since = "1.0.1", forRemoval = true)
public final class ByteTool {

    private ByteTool() {
        // utility class
    }

    /**
     * Counts how many leading bytes are identical in both arrays.
     *
     * @param first  the first byte array
     * @param second the second byte array
     * @return number of matching bytes from the beginning of the arrays
     * @deprecated Prefer
     *             {@link org.hestiastore.index.bytes.ByteTool#countMatchingPrefixBytes(org.hestiastore.index.bytes.ByteSequence, org.hestiastore.index.bytes.ByteSequence)}.
     */
    @Deprecated(since = "1.0.1", forRemoval = true)
    public static int countMatchingPrefixBytes(final byte[] first,
            final byte[] second) {
        Vldtn.requireNonNull(first, "first");
        Vldtn.requireNonNull(second, "second");

        final int limit = Math.min(first.length, second.length);
        int index = 0;
        while (index < limit && first[index] == second[index]) {
            index++;
        }
        return index;
    }

    /**
     * Returns a slice of {@code full} starting at {@code index}.
     *
     * @param index start index (inclusive)
     * @param full  source byte array
     * @return remaining bytes from {@code index} to the end of {@code full}
     * @throws IllegalArgumentException if {@code index} is out of range
     * @deprecated Prefer
     *             {@link org.hestiastore.index.bytes.ByteTool#getRemainingBytesAfterIndex(int, org.hestiastore.index.bytes.ByteSequence)}
     *             or {@code Arrays.copyOfRange(full, index, full.length)}.
     */
    @Deprecated(since = "1.0.1", forRemoval = true)
    public static byte[] getRemainingBytesAfterIndex(final int index,
            final byte[] full) {
        Vldtn.requireNonNull(full, "full");
        if (index < 0 || index > full.length) {
            throw new IllegalArgumentException(String.format(
                    "Index '%d' is out of range 0..%d", index, full.length));
        }
        final byte[] out = new byte[full.length - index];
        System.arraycopy(full, index, out, 0, out.length);
        return out;
    }

    /**
     * Concatenates two byte arrays into a new array containing the contents of
     * {@code first} followed by {@code second}.
     *
     * @param first  non-null first array
     * @param second non-null second array
     * @return new array comprising both inputs
     * @throws IllegalArgumentException if any argument is {@code null}
     * @deprecated Prefer
     *             {@link org.hestiastore.index.bytes.ByteTool#concatenate(org.hestiastore.index.bytes.ByteSequence, org.hestiastore.index.bytes.ByteSequence)}
     *             when working with indexed byte data.
     */
    @Deprecated(since = "1.0.1", forRemoval = true)
    public static byte[] concatenate(final byte[] first,
            final byte[] second) {
        Vldtn.requireNonNull(first, "first");
        Vldtn.requireNonNull(second, "second");
        final byte[] out = new byte[first.length + second.length];
        System.arraycopy(first, 0, out, 0, first.length);
        System.arraycopy(second, 0, out, first.length, second.length);
        return out;
    }
}
