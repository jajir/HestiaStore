package org.hestiastore.index.datatype;

import java.util.Arrays;

import org.hestiastore.index.Vldtn;

/**
 * Represents a byte array as a comparable object.
 */
public class ByteArray implements Comparable<ByteArray> {

    private final byte[] data;

    /**
     * Creates an immutable byte-array value by copying input bytes.
     *
     * @param data source bytes
     * @return immutable {@link ByteArray}
     */
    public static ByteArray of(final byte[] data) {
        return new ByteArray(data);
    }

    private ByteArray(final byte[] data) {
        Vldtn.requireNonNull(data, "byteArray");
        this.data = Arrays.copyOf(data, data.length);
    }

    /**
     * Returns a defensive copy of wrapped bytes.
     *
     * @return copied bytes
     */
    public byte[] getBytes() {
        return Arrays.copyOf(data, data.length);
    }

    /**
     * Returns number of bytes in this value.
     *
     * @return byte length
     */
    public int length() {
        return data.length;
    }

    /**
     * Copies all bytes into destination from offset {@code 0}.
     *
     * @param destination destination buffer
     * @return number of copied bytes
     */
    public int copyTo(final byte[] destination) {
        return copyTo(destination, 0);
    }

    /**
     * Copies all bytes into destination at the provided offset.
     *
     * @param destination destination buffer
     * @param destinationOffset destination start offset
     * @return number of copied bytes
     */
    public int copyTo(final byte[] destination, final int destinationOffset) {
        Vldtn.requireNonNull(destination, "destination");
        if (destinationOffset < 0 || destinationOffset > destination.length) {
            throw new IllegalArgumentException(String.format(
                    "Property 'destinationOffset' must be between 0 and %s (inclusive). Got: %s",
                    destination.length, destinationOffset));
        }
        final int available = destination.length - destinationOffset;
        if (available < data.length) {
            throw new IllegalArgumentException(String.format(
                    "Destination buffer too small. Required '%s' but was '%s'",
                    data.length, available));
        }
        System.arraycopy(data, 0, destination, destinationOffset, data.length);
        return data.length;
    }

    /**
     * Compares byte arrays lexicographically using unsigned-byte semantics.
     *
     * @param other value to compare
     * @return negative, zero or positive comparison result
     */
    @Override
    public int compareTo(final ByteArray other) {
        int len = Math.min(this.data.length, other.data.length);
        for (int i = 0; i < len; i++) {
            int a = this.data[i] & 0xFF;
            int b = other.data[i] & 0xFF;
            if (a != b)
                return a - b;
        }
        return this.data.length - other.data.length;
    }

    /**
     * Returns whether this value contains exactly the same bytes as another
     * {@link ByteArray}.
     *
     * @param o object to compare
     * @return {@code true} when bytes are equal
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof ByteArray))
            return false;
        ByteArray that = (ByteArray) o;
        return Arrays.equals(this.data, that.data);
    }

    /**
     * Returns hash code derived from underlying bytes.
     *
     * @return hash code
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    /**
     * Returns lowercase hexadecimal representation for diagnostics.
     *
     * @return string representation
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ByteArray[");
        for (byte b : data) {
            sb.append(String.format("%02x", b));
        }
        sb.append("]");
        return sb.toString();
    }
}
