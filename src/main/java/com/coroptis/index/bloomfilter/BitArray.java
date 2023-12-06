package com.coroptis.index.bloomfilter;

import java.util.Arrays;

public class BitArray {

    private final byte[] byteArray;

    public BitArray(final int length) {
        byteArray = new byte[length];
    }

    public boolean setBit(final int index) {
        if (index < 0 || index >= byteArray.length * 8) {
            throw new IndexOutOfBoundsException("Invalid index");
        }

        int byteIndex = index / 8;
        int bitIndex = index % 8;

        byte oldValue = byteArray[byteIndex];
        byte newValue = (byte) (oldValue | (1 << bitIndex));
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

        byte oldValue = byteArray[byteIndex];
        byte newValue = (byte) (oldValue | (1 << bitIndex));

        return oldValue != newValue;
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
