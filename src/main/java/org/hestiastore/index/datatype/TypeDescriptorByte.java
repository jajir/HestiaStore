package org.hestiastore.index.datatype;

import java.util.Comparator;

import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.MutableBytes;
import org.hestiastore.index.Vldtn;

public class TypeDescriptorByte implements TypeDescriptor<Byte> {

    /**
     * Thombstone value, use can't use it.
     */
    private static final Byte TOMBSTONE_VALUE = Byte.MIN_VALUE;

    @Override
    public ConvertorToBytes<Byte> getConvertorToBytes() {
        return this::getBytesBuffer;
    }

    @Override
    public ConvertorFromBytes<Byte> getConvertorFromBytes() {
        return bytes -> {
            Vldtn.requireNonNull(bytes, "bytes");
            if (bytes.length() != 1) {
                throw new IllegalArgumentException(
                        "Byte value requires exactly one byte");
            }
            return bytes.getByte(0);
        };
    }

    @Override
    public TypeReader<Byte> getTypeReader() {
        return reader -> {
            final int read = reader.read();
            return read == -1 ? null : (byte) read;
        };
    }

    @Override
    public TypeWriter<Byte> getTypeWriter() {
        return (fileWriter, b) -> {
            fileWriter.write(b);
            return 1;
        };
    }

    @Override
    public Comparator<Byte> getComparator() {
        return (i1, i2) -> i2 - i1;
    }

    @Override
    public Byte getTombstone() {
        return TOMBSTONE_VALUE;
    }

    private ByteSequence getBytesBuffer(final Byte value) {
        Vldtn.requireNonNull(value, "value");
        final MutableBytes buffer = MutableBytes.allocate(1);
        buffer.setByte(0, value.byteValue());
        return buffer.toBytes();
    }

}
