package org.hestiastore.index.datatype;

import java.util.Comparator;

public class TypeDescriptorDouble implements TypeDescriptor<Double> {

    /**
     * Tombstone value, use can't use it.
     */
    public static final Double TOMBSTONE_VALUE = Double.MAX_VALUE - 1;

    /**
     * How many bytes is required to store Float.
     */
    private static final int REQUIRED_BYTES = 8;

    @Override
    public ConvertorToBytes<Double> getConvertorToBytes() {
        return object -> getBytes(object);
    }

    @Override
    public ConvertorFromBytes<Double> getConvertorFromBytes() {
        return bytes -> load(bytes, 0);
    }

    @Override
    public TypeReader<Double> getTypeReader() {
        return fileReader -> {
            final byte[] bytes = new byte[REQUIRED_BYTES];
            if (fileReader.read(bytes) == -1) {
                return null;
            }
            return load(bytes, 0);
        };
    }

    Double load(byte[] bytes, int offset) {
        if (bytes.length < offset + REQUIRED_BYTES) {
            throw new IllegalArgumentException(
                    "Not enough bytes to read a Float value");
        }
        long bits = ((long) (bytes[offset] & 0xFF) << 56)
                | ((long) (bytes[offset + 1] & 0xFF) << 48)
                | ((long) (bytes[offset + 2] & 0xFF) << 40)
                | ((long) (bytes[offset + 3] & 0xFF) << 32)
                | ((long) (bytes[offset + 4] & 0xFF) << 24)
                | ((long) (bytes[offset + 5] & 0xFF) << 16)
                | ((long) (bytes[offset + 6] & 0xFF) << 8)
                | (bytes[offset + 7] & 0xFF);
        return Double.longBitsToDouble(bits);
    }

    byte[] getBytes(Double object) {
        if (object == null) {
            throw new IllegalArgumentException("Object can't be null");
        }

        long bits = Double.doubleToLongBits(object);
        return new byte[] { //
                (byte) (bits >> 56), //
                (byte) (bits >> 48), //
                (byte) (bits >> 40), //
                (byte) (bits >> 32), //
                (byte) (bits >> 24), //
                (byte) (bits >> 16), //
                (byte) (bits >> 8), //
                (byte) bits //
        };
    }

    @Override
    public Comparator<Double> getComparator() {
        return Double::compareTo;
    }

    @Override
    public TypeWriter<Double> getTypeWriter() {
        return (writer, object) -> {
            writer.write(getBytes(object));
            return REQUIRED_BYTES;
        };
    }

    @Override
    public Double getTombstone() {
        return TOMBSTONE_VALUE;
    }
}
