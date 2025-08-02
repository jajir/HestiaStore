package org.hestiastore.index;

import java.util.Arrays;

/**
 * Block of data bytes.
 * 
 * Main purpose is to encapsulate byte array and provide utility methods.
 */
public class Bytes {

    private final byte[] data;

    public static Bytes of(final byte[] data) {
        return new Bytes(data);
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

    public byte[] getData() {
        return data;
    }

    public int length() {
        return data.length;
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

}
