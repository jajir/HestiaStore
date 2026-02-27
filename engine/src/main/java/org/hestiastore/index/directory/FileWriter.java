package org.hestiastore.index.directory;

import java.util.Arrays;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.Vldtn;

/**
 * Generic byte sink used by serializers.
 */
public interface FileWriter extends CloseableResource {

    /**
     * Writes a single byte.
     *
     * @param b byte to write
     */
    void write(byte b);

    /**
     * Writes the entire byte array.
     *
     * @param bytes bytes to write
     */
    void write(byte[] bytes);

    /**
     * Writes a sub-range of the provided byte array.
     *
     * @param bytes  source byte array
     * @param offset start offset in {@code bytes}
     * @param length number of bytes to write
     */
    default void write(final byte[] bytes, final int offset,
            final int length) {
        final byte[] validated = Vldtn.requireNonNull(bytes, "bytes");
        final int from = Vldtn.requireGreaterThanOrEqualToZero(offset,
                "offset");
        final int len = Vldtn.requireGreaterThanOrEqualToZero(length,
                "length");
        if (from > validated.length || from + len > validated.length) {
            throw new IllegalArgumentException(String.format(
                    "Offset '%s' and length '%s' exceed source length '%s'",
                    from, len, validated.length));
        }
        if (len == 0) {
            return;
        }
        if (from == 0 && len == validated.length) {
            write(validated);
            return;
        }
        write(Arrays.copyOfRange(validated, from, from + len));
    }
}
