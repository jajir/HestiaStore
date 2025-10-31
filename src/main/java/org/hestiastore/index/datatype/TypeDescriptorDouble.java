package org.hestiastore.index.datatype;

import java.util.Comparator;

import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.MutableBytes;

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
            final MutableBytes buffer = MutableBytes.allocate(REQUIRED_BYTES);
            if (fileReader.read(buffer) == -1) {
                return null;
            }
            return load(buffer, 0);
        };
    }

    Double load(ByteSequence bytes, int offset) {
        if (bytes.length() < offset + REQUIRED_BYTES) {
            throw new IllegalArgumentException(
                    "Not enough bytes to read a Float value");
        }
        long bits = ((long) (bytes.getByte(offset) & 0xFF) << 56)
                | ((long) (bytes.getByte(offset + 1) & 0xFF) << 48)
                | ((long) (bytes.getByte(offset + 2) & 0xFF) << 40)
                | ((long) (bytes.getByte(offset + 3) & 0xFF) << 32)
                | ((long) (bytes.getByte(offset + 4) & 0xFF) << 24)
                | ((long) (bytes.getByte(offset + 5) & 0xFF) << 16)
                | ((long) (bytes.getByte(offset + 6) & 0xFF) << 8)
                | (bytes.getByte(offset + 7) & 0xFF);
        return Double.longBitsToDouble(bits);
    }

    ByteSequence getBytesBuffer(Double object) {
        if (object == null) {
            throw new IllegalArgumentException("Object can't be null");
        }

        long bits = Double.doubleToLongBits(object);
        final MutableBytes out = MutableBytes.allocate(REQUIRED_BYTES);
        out.setByte(0, (byte) (bits >> 56));
        out.setByte(1, (byte) (bits >> 48));
        out.setByte(2, (byte) (bits >> 40));
        out.setByte(3, (byte) (bits >> 32));
        out.setByte(4, (byte) (bits >> 24));
        out.setByte(5, (byte) (bits >> 16));
        out.setByte(6, (byte) (bits >> 8));
        out.setByte(7, (byte) bits);
        return out.toBytes();
    }

    @Override
    public Comparator<Double> getComparator() {
        return Double::compareTo;
    }

    @Override
    public TypeWriter<Double> getTypeWriter() {
        return (writer, object) -> {
            final ByteSequence encoded = getBytesBuffer(object);
            writer.write(encoded);
            return encoded.length();
        };
    }

    @Override
    public Double getTombstone() {
        return TOMBSTONE_VALUE;
    }
}
