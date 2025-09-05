package org.hestiastore.index;

import java.util.Arrays;

/**
 * Block of data bytes.
 * 
 * Main purpose is to encapsulate byte array and provide utility methods.
 */
public class Bytes {

    public static final Bytes EMPTY = new Bytes(new byte[0]);

    private final byte[] data;

    public static Bytes of(final byte[] data) {
        return new Bytes(data);
    }

    public static Bytes of(final Bytes data1, final Bytes data2) {
        Vldtn.requireNonNull(data1, "data1");
        Vldtn.requireNonNull(data2, "data2");
        final byte[] combined = new byte[data1.length() + data2.length()];
        System.arraycopy(data1.getData(), 0, combined, 0, data1.length());
        System.arraycopy(data2.getData(), 0, combined, data1.length(),
                data2.length());
        return new Bytes(combined);
    }

    private Bytes(final byte[] data) {
        this.data = Vldtn.requireNonNull(data, "data");
    }

    public Bytes subBytes(int startByte, int endByte) {
        Vldtn.requireBetween(startByte, 0, data.length, "startByte");
        Vldtn.requireBetween(endByte, 0, data.length, "endByte");
        if (startByte > endByte) {
            throw new IllegalArgumentException(
                    "startByte must be less than or equal to endByte");
        }
        final int len = endByte - startByte;
        byte[] data = new byte[len];
        System.arraycopy(this.data, startByte, data, 0, len);
        return new Bytes(data);
    }

    /**
     * Returns a new Bytes instance padded with zeros to the specified size. If
     * the current size is greater than or equal to newSize, returns this.
     *
     * @param newSize the target size of the resulting Bytes object
     * @return a new Bytes instance with data padded to newSize
     */
    public Bytes paddedTo(final int newSize) {
        Vldtn.requireBetween(newSize, 0, Integer.MAX_VALUE, "newSize");
        if (data.length >= newSize) {
            return this;
        }
        byte[] padded = new byte[newSize];
        System.arraycopy(data, 0, padded, 0, data.length);
        return new Bytes(padded);
    }

    public Bytes add(final Bytes bytes) {
        return Bytes.of(this, bytes);
    }

    public byte[] getData() {
        return data;
    }

    public int length() {
        return data.length;
    }

    public Bytes paddedToNextCell() {
        int modulo = data.length % 16;
        if (modulo == 0) {
            return this;
        }
        int padding = 16 - modulo;
        byte[] padded = new byte[data.length + padding];
        System.arraycopy(data, 0, padded, 0, data.length);
        return new Bytes(padded);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Bytes)) {
            return false;
        }
        Bytes other = (Bytes) obj;
        return Arrays.equals(data, other.data);
    }

    @Override
    public String toString() {
        return "Bytes{" + "data=" + Arrays.toString(data) + '}';
    }

}
