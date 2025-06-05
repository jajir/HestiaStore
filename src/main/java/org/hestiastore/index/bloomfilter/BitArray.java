package org.hestiastore.index.bloomfilter;

import java.util.Arrays;
import java.util.Objects;

public class BitArray {

    private final byte[] byteArray;

    public BitArray(final int length) {
        byteArray = new byte[length];
    }

    public BitArray(final byte[] data) {
        byteArray = Objects.requireNonNull(data, "Data are null");
    }

    /**
     * Sets the bit at the specified index to 1.
     *
     * @param index the index of the bit to set
     * @return true if the bit was changed, false if it was already set
     * @throws IndexOutOfBoundsException if the index is out of bounds
     */
    public boolean setBit(final int index) {
        if (index < 0 || index >= byteArray.length * 8) {
            throw new IndexOutOfBoundsException("Invalid index");
        }

        int byteIndex = index / 8;
        int bitIndex = index % 8;

        byte oldValue = byteArray[byteIndex];
        byte newValue = (byte) (oldValue & 0xff | (1 << bitIndex));

        byteArray[byteIndex] = newValue;

        return oldValue != newValue;
    }

    public byte[] getByteArray() {
        return byteArray;
    }

    public boolean get(final int index) {
        if (index < 0 || index >= byteArray.length * 8) {
            throw new IndexOutOfBoundsException("Invalid index");
        }

        int byteIndex = index / 8;
        int bitIndex = index % 8;

        int b = byteArray[byteIndex] & 0xff; // Convert byte to unsigned int

        return (b & (1 << bitIndex)) != 0;
    }

    public int bitSize() {
        return byteArray.length * 8;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other)
            return true;
        if (!(other instanceof BitArray))
            return false;
        BitArray that = (BitArray) other;
        return Arrays.equals(byteArray, that.byteArray);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(byteArray);
    }
}
