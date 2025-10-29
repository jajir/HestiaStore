package org.hestiastore.index.datatype;

import java.util.Comparator;

import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.MutableBytes;

public class TypeDescriptorFloat implements TypeDescriptor<Float> {

    /**
     * Tombstone value, use can't use it.
     */
    public static final Float TOMBSTONE_VALUE = Float.MAX_VALUE - 1;

    /**
     * How many bytes is required to store Float.
     */
    private static final int REQUIRED_BYTES = 4;

    @Override
    public ConvertorToBytes<Float> getConvertorToBytes() {
        return this::getBytesBuffer;
    }

    @Override
    public ConvertorFromBytes<Float> getConvertorFromBytes() {
        return bytes -> load(bytes, 0);
    }

    @Override
    public TypeReader<Float> getTypeReader() {
        return fileReader -> {
            final MutableBytes buffer = MutableBytes.allocate(REQUIRED_BYTES);
            if (fileReader.read(buffer) == -1) {
                return null;
            }
            return load(buffer, 0);
        };
    }

    Float load(ByteSequence bytes, int offset) {
        if (bytes.length() < offset + REQUIRED_BYTES) {
            throw new IllegalArgumentException(
                    "Not enough bytes to read a Float value");
        }
        int bits = ((bytes.getByte(offset) & 0xFF) << 24)
                | ((bytes.getByte(offset + 1) & 0xFF) << 16)
                | ((bytes.getByte(offset + 2) & 0xFF) << 8)
                | (bytes.getByte(offset + 3) & 0xFF);
        return Float.intBitsToFloat(bits);
    }

    Bytes getBytesBuffer(Float object) {
        if (object == null) {
            throw new IllegalArgumentException("Object can't be null");
        }
        int bits = Float.floatToIntBits(object);
        final MutableBytes out = MutableBytes.allocate(REQUIRED_BYTES);
        out.setByte(0, (byte) (bits >> 24));
        out.setByte(1, (byte) (bits >> 16));
        out.setByte(2, (byte) (bits >> 8));
        out.setByte(3, (byte) bits);
        return out.toBytes();
    }

    @Override
    public Comparator<Float> getComparator() {
        return Float::compareTo;
    }

    @Override
    public TypeWriter<Float> getTypeWriter() {
        return (writer, object) -> {
            final Bytes encoded = getBytesBuffer(object);
            writer.write(encoded);
            return encoded.length();
        };
    }

    @Override
    public Float getTombstone() {
        return TOMBSTONE_VALUE;
    }

}
