package org.hestiastore.index.bytes;

import java.util.Arrays;

import org.hestiastore.index.Vldtn;

/**
 * Block of data bytes.
 * 
 * Main purpose is to encapsulate byte array and provide utility methods.
 */
public class Bytes implements ByteSequence {

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
     * Create new Bytes instance that copies the provided byte array.
     *
     * @param data required byte array
     * @return created Bytes instance backed by a copy of the data
     */
    public static Bytes copyOf(final byte[] data) {
        final byte[] safeData = Vldtn.requireNonNull(data, "data");
        return new Bytes(Arrays.copyOf(safeData, safeData.length));
    }

    /**
     * Create new Bytes instance by copying a {@link ByteSequence}.
     *
     * @param sequence source byte sequence
     * @return created Bytes instance backed by a copy of the sequence data
     */
    public static Bytes copyOf(final ByteSequence sequence) {
        Vldtn.requireNonNull(sequence, "sequence");
        if (sequence.isEmpty()) {
            return EMPTY;
        }
        return new Bytes(sequence.toByteArray());
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
        final MutableBytes combined = MutableBytes
                .allocate(data1.length() + data2.length());
        combined.setBytes(0, data1);
        combined.setBytes(data1.length(), data2);
        return combined.toImmutableBytes();
    }

    private Bytes(final byte[] data) {
        this.data = Vldtn.requireNonNull(data, "data");
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

    @Override
    public byte[] toByteArray() {
        return data;
    }

    /**
     * Returns the length of the byte array.
     * 
     * @return the length of the byte array
     */
    @Override
    public int length() {
        return data.length;
    }

    @Override
    public byte getByte(final int index) {
        final int maxIndex = data.length - 1;
        if (index < 0 || index > maxIndex) {
            throw new IllegalArgumentException(String.format(
                    "Property 'index' must be between 0 and %d (inclusive). Got: %d",
                    maxIndex, index));
        }
        return data[index];
    }

    @Override
    public void copyTo(final int sourceOffset, final byte[] target,
            final int targetOffset, final int length) {
        Vldtn.requireNonNull(target, "target");
        if (sourceOffset < 0 || length < 0 || sourceOffset > data.length
                || sourceOffset + length > data.length) {
            throw new IllegalArgumentException(String.format(
                    "Property 'sourceOffset' with length %d exceeds capacity %d",
                    length, data.length));
        }
        if (targetOffset < 0 || targetOffset > target.length
                || targetOffset + length > target.length) {
            throw new IllegalArgumentException(String.format(
                    "Property 'targetOffset' with length %d exceeds capacity %d",
                    length, target.length));
        }
        if (length == 0) {
            return;
        }
        System.arraycopy(data, sourceOffset, target, targetOffset, length);
    }

    @Override
    public ByteSequence slice(final int fromInclusive, final int toExclusive) {
        Vldtn.requireBetween(fromInclusive, 0, data.length, "fromInclusive");
        Vldtn.requireBetween(toExclusive, 0, data.length, "toExclusive");
        if (fromInclusive > toExclusive) {
            throw new IllegalArgumentException(
                    "fromInclusive must be less than or equal to toExclusive");
        }
        if (fromInclusive == toExclusive) {
            return Bytes.EMPTY;
        }
        return ByteSequenceView.of(data, fromInclusive, toExclusive);
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
