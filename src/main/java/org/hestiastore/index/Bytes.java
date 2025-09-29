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

    /**
     * Create new Bytes instance from byte array.
     * 
     * @param data required byte array
     * @return created Bytes instance
     */
    public static Bytes of(final byte[] data) {
        return new Bytes(data);
    }

    /**
     * Allocate new Bytes instance with specified size.
     * 
     * @param size required size of the byte array
     * @return created Bytes instance
     */
    public static Bytes allocate(int size) {
        return new Bytes(new byte[size]);
    }

    /**
     * Concatenate two Bytes instances into a new one.
     * 
     * @param data1 first Bytes instance
     * @param data2 second Bytes instance
     * @return created Bytes instance
     */
    public static Bytes concat(final Bytes data1, final Bytes data2) {
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

    /**
     * Returns a new Bytes instance that is a subarray of this instance,
     * starting from startByte (inclusive) to endByte (exclusive).
     * 
     * @param startByte required start byte index (inclusive)
     * @param endByte   required end byte index (exclusive)
     * @return
     */
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

    /**
     * Concatenate this Bytes instance with another one and return a new Bytes
     * instance.
     * 
     * @param bytes required Bytes instance to concatenate
     * @return created Bytes instance
     */
    public Bytes concat(final Bytes bytes) {
        return Bytes.concat(this, bytes);
    }

    /**
     * Returns the underlying byte array.
     * 
     * @return the underlying byte array
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Returns the length of the byte array.
     * 
     * @return the length of the byte array
     */
    public int length() {
        return data.length;
    }

    /**
     * 
     * Returns a new Bytes instance padded with zeros to the next multiple of 16
     * bytes. If the current size is already a multiple of 16, returns this.
     * 
     * @return a new Bytes instance padded to the next multiple of 16 bytes
     */
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
