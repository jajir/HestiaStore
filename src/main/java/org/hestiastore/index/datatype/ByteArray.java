package org.hestiastore.index.datatype;

import java.util.Arrays;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

/**
 * Represents a byte array as a comparable object.
 *
 * @deprecated prefer using {@link org.hestiastore.index.Bytes} directly.
 */
@Deprecated(forRemoval = true)
public class ByteArray implements Comparable<ByteArray> {

    private final byte[] data;

    public static ByteArray of(final byte[] data) {
        return new ByteArray(data);
    }

    public static ByteArray of(final Bytes bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        return new ByteArray(bytes.toByteArray());
    }

    private ByteArray(final byte[] data) {
        Vldtn.requireNonNull(data, "byteArray");
        this.data = Arrays.copyOf(data, data.length);
    }

    public byte[] getBytes() {
        return Arrays.copyOf(data, data.length);
    }

    public Bytes toBytes() {
        return Bytes.copyOf(data);
    }

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

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof ByteArray))
            return false;
        ByteArray that = (ByteArray) o;
        return Arrays.equals(this.data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

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
