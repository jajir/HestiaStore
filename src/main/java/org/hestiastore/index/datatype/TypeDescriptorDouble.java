package org.hestiastore.index.datatype;

import java.util.Comparator;

import org.hestiastore.index.Bytes;

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
        return this::getBytesBuffer;
    }

    @Override
    public ConvertorFromBytes<Double> getConvertorFromBytes() {
        return bytes -> load(bytes, 0);
    }

    @Override
    public TypeReader<Double> getTypeReader() {
        return fileReader -> {
            final Bytes buffer = Bytes.allocate(REQUIRED_BYTES);
            if (fileReader.read(buffer) == -1) {
                return null;
            }
            return load(buffer, 0);
        };
    }

    Double load(Bytes bytes, int offset) {
        if (bytes.length() < offset + REQUIRED_BYTES) {
            throw new IllegalArgumentException(
                    "Not enough bytes to read a Float value");
        }
        final byte[] raw = bytes.getData();
        long bits = ((long) (raw[offset] & 0xFF) << 56)
                | ((long) (raw[offset + 1] & 0xFF) << 48)
                | ((long) (raw[offset + 2] & 0xFF) << 40)
                | ((long) (raw[offset + 3] & 0xFF) << 32)
                | ((long) (raw[offset + 4] & 0xFF) << 24)
                | ((long) (raw[offset + 5] & 0xFF) << 16)
                | ((long) (raw[offset + 6] & 0xFF) << 8)
                | (raw[offset + 7] & 0xFF);
        return Double.longBitsToDouble(bits);
    }

    Bytes getBytesBuffer(Double object) {
        if (object == null) {
            throw new IllegalArgumentException("Object can't be null");
        }

        long bits = Double.doubleToLongBits(object);
        final Bytes out = Bytes.allocate(REQUIRED_BYTES);
        final byte[] data = out.getData();
        data[0] = (byte) (bits >> 56);
        data[1] = (byte) (bits >> 48);
        data[2] = (byte) (bits >> 40);
        data[3] = (byte) (bits >> 32);
        data[4] = (byte) (bits >> 24);
        data[5] = (byte) (bits >> 16);
        data[6] = (byte) (bits >> 8);
        data[7] = (byte) bits;
        return out;
    }

    @Override
    public Comparator<Double> getComparator() {
        return Double::compareTo;
    }

    @Override
    public TypeWriter<Double> getTypeWriter() {
        return (writer, object) -> {
            final Bytes encoded = getBytesBuffer(object);
            writer.write(encoded);
            return encoded.length();
        };
    }

    @Override
    public Double getTombstone() {
        return TOMBSTONE_VALUE;
    }
}
