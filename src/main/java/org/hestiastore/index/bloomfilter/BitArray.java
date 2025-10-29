package org.hestiastore.index.bloomfilter;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

public class BitArray {

    private final Bytes bytes;

    public BitArray(final int length) {
        this(Bytes.allocate(length));
    }

    public BitArray(final Bytes data) {
        this.bytes = Vldtn.requireNonNull(data, "data");
    }

    /**
     * Sets the bit at the specified index to 1.
     *
     * @param index the index of the bit to set
     * @return true if the bit was changed, false if it was already set
     * @throws IndexOutOfBoundsException if the index is out of bounds
     */
    public boolean setBit(final int index) {
        final byte[] data = bytes.getData();
        if (index < 0 || index >= data.length * 8) {
            throw new IndexOutOfBoundsException("Invalid index");
        }

        int byteIndex = index / 8;
        int bitIndex = index % 8;

        int oldValue = data[byteIndex] & 0xff;
        int newValue = oldValue | (1 << bitIndex);

        if (oldValue == newValue) {
            return false; // Bit was already set
        } else {
            data[byteIndex] = (byte) newValue;
            return true;
        }
        //FIXME set one byte in Bytes
    }

    public boolean get(final int index) {
        final byte[] data = bytes.getData();
        if (index < 0 || index >= data.length * 8) {
            throw new IndexOutOfBoundsException("Invalid index");
        }

        int byteIndex = index / 8;
        int bitIndex = index % 8;

        int b = data[byteIndex] & 0xff; // Convert byte to unsigned int

        return (b & (1 << bitIndex)) != 0;
    }

    public int bitSize() {
        return bytes.length() * 8;
    }

    public Bytes getBytes() {
        return bytes;
    }

    @Override
    public int hashCode() {
        return bytes.hashCode();
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof BitArray)) {
            return false;
        }
        BitArray that = (BitArray) other;
        return bytes.equals(that.bytes);
    }
}
