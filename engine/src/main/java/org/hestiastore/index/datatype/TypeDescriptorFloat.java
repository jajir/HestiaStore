package org.hestiastore.index.datatype;

import java.util.Comparator;

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
        return object -> getBytes(object);
    }

    @Override
    public ConvertorFromBytes<Float> getConvertorFromBytes() {
        return bytes -> load(bytes, 0);
    }

    @Override
    public TypeReader<Float> getTypeReader() {
        return fileReader -> {
            final byte[] bytes = new byte[REQUIRED_BYTES];
            if (fileReader.read(bytes) == -1) {
                return null;
            }
            return load(bytes, 0);
        };
    }

    Float load(byte[] bytes, int offset) {
        if (bytes.length < offset + REQUIRED_BYTES) {
            throw new IllegalArgumentException(
                    "Not enough bytes to read a Float value");
        }
        int bits = ((bytes[offset] & 0xFF) << 24)
                | ((bytes[offset + 1] & 0xFF) << 16)
                | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
        return Float.intBitsToFloat(bits);
    }

    byte[] getBytes(Float object) {
        if (object == null) {
            throw new IllegalArgumentException("Object can't be null");
        }
        int bits = Float.floatToIntBits(object);
        return new byte[] { //
                (byte) (bits >> 24), //
                (byte) (bits >> 16), //
                (byte) (bits >> 8), //
                (byte) bits //
        };
    }

    @Override
    public Comparator<Float> getComparator() {
        return Float::compareTo;
    }

    @Override
    public TypeWriter<Float> getTypeWriter() {
        return (writer, object) -> {
            writer.write(getBytes(object));
            return REQUIRED_BYTES;
        };
    }

    @Override
    public Float getTombstone() {
        return TOMBSTONE_VALUE;
    }

}
