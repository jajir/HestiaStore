package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;

/**
 * Holds encoded byte payload and effective encoded length.
 */
@SuppressWarnings("java:S6206")
public final class EncodedBytes {

    private final byte[] bytes;

    private final int length;

    public EncodedBytes(final byte[] bytes, final int length) {
        this.bytes = Vldtn.requireNonNull(bytes, "bytes");
        this.length = Vldtn.requireGreaterThanOrEqualToZero(length, "length");
        if (length > bytes.length) {
            throw new IllegalArgumentException(String.format(
                    "Encoded length '%s' exceeds byte array size '%s'", length,
                    bytes.length));
        }
    }

    public byte[] getBytes() {
        return bytes;
    }

    public int getLength() {
        return length;
    }
}
