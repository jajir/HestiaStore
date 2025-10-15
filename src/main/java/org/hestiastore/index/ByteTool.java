package org.hestiastore.index;

import java.util.Objects;

/**
 * Utility helpers for working with byte arrays.
 */
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
     */
    public static int countMatchingPrefixBytes(final byte[] first,
            final byte[] second) {
        Objects.requireNonNull(first, "First byte array is null.");
        Objects.requireNonNull(second, "Second byte array is null.");

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
     */
    public static byte[] getRemainingBytesAfterIndex(final int index,
            final byte[] full) {
        Objects.requireNonNull(full, "Byte array must not be null.");
        if (index < 0 || index > full.length) {
            throw new IllegalArgumentException(String.format(
                    "Index '%d' is out of range 0..%d", index, full.length));
        }
        final byte[] out = new byte[full.length - index];
        System.arraycopy(full, index, out, 0, out.length);
        return out;
    }
}
