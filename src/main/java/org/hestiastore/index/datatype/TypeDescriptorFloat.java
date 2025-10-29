package org.hestiastore.index.datatype;

import java.util.Comparator;

import org.hestiastore.index.Bytes;

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
            final Bytes buffer = Bytes.allocate(REQUIRED_BYTES);
            if (fileReader.read(buffer) == -1) {
                return null;
            }
            return load(buffer, 0);
        };
    }

    Float load(Bytes bytes, int offset) {
        if (bytes.length() < offset + REQUIRED_BYTES) {
            throw new IllegalArgumentException(
                    "Not enough bytes to read a Float value");
        }
        final byte[] raw = bytes.getData();
        int bits = ((raw[offset] & 0xFF) << 24)
                | ((raw[offset + 1] & 0xFF) << 16)
                | ((raw[offset + 2] & 0xFF) << 8) | (raw[offset + 3] & 0xFF);
        return Float.intBitsToFloat(bits);
    }

    Bytes getBytesBuffer(Float object) {
        if (object == null) {
            throw new IllegalArgumentException("Object can't be null");
        }
        int bits = Float.floatToIntBits(object);
        final Bytes out = Bytes.allocate(REQUIRED_BYTES);
        final byte[] data = out.getData();
        data[0] = (byte) (bits >> 24);
        data[1] = (byte) (bits >> 16);
        data[2] = (byte) (bits >> 8);
        data[3] = (byte) bits;
        return out;
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
