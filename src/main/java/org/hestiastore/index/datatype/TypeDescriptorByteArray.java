package org.hestiastore.index.datatype;

import java.nio.charset.Charset;
import java.util.Comparator;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;

public class TypeDescriptorByteArray
        implements TypeDescriptor<ByteSequence> {

    private static final String CHARSET_ENCODING_NAME = "ISO_8859_1";

    private static final Charset CHARSET_ENCODING = Charset
            .forName(CHARSET_ENCODING_NAME);

    /**
     * Tombstones value, use can't use it.
     */
    public static final ByteSequence TOMBSTONE_VALUE = ByteSequences
            .copyOf("(*&^%$#@!)-1eaa9b2c-3c11-11ee-be56-0242ac120002"
                    .getBytes(CHARSET_ENCODING));

    @Override
    public ConvertorFromBytes<ByteSequence> getConvertorFromBytes() {
        return bytes -> {
            final ByteSequence validated = Vldtn.requireNonNull(bytes, "bytes");
            return ByteSequences.copyOf(validated);
        };
    }

    @Override
    public ConvertorToBytes<ByteSequence> getConvertorToBytes() {
        return data -> {
            final ByteSequence validated = Vldtn.requireNonNull(data, "bytes");
            return ByteSequences.copyOf(validated);
        };
    }

    @Override
    public VarLengthWriter<ByteSequence> getTypeWriter() {
        return new VarLengthWriter<>(getConvertorToBytes());
    }

    @Override
    public VarLengthReader<ByteSequence> getTypeReader() {
        return new VarLengthReader<>(getConvertorFromBytes());
    }

    @Override
    public Comparator<ByteSequence> getComparator() {
        return (left, right) -> {
            final ByteSequence leftValidated = Vldtn.requireNonNull(left,
                    "left");
            final ByteSequence rightValidated = Vldtn.requireNonNull(right,
                    "right");
            final int leftLength = leftValidated.length();
            final int rightLength = rightValidated.length();
            final int limit = Math.min(leftLength, rightLength);
            for (int i = 0; i < limit; i++) {
                final int a = leftValidated.getByte(i) & 0xFF;
                final int b = rightValidated.getByte(i) & 0xFF;
                if (a != b) {
                    return a - b;
                }
            }
            return leftLength - rightLength;
        };
    }

    @Override
    public ByteSequence getTombstone() {
        return TOMBSTONE_VALUE;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        return obj != null && getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

}
