package org.hestiastore.index.datatype;

import java.nio.charset.Charset;
import java.util.Comparator;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.Bytes;
import org.hestiastore.index.Vldtn;

/**
 * TypeDescriptor for String values. It uses ISO_8859_1 encoding. Max length of
 * string is over 2 GB, exactly Integer.MAX_VALUE.
 */
public class TypeDescriptorString implements TypeDescriptor<String> {

    private static final String CHARSET_ENCODING_NAME = "ISO_8859_1";

    private static final Charset CHARSET_ENCODING = Charset
            .forName(CHARSET_ENCODING_NAME);

    /**
     * Tombstones value, use can't use it.
     */
    public static final String TOMBSTONE_VALUE = "(*&^%$#@!)-1eaa9b2c-3c11-11ee-be56-0242ac120002";

    @Override
    public ConvertorFromBytes<String> getConvertorFromBytes() {
        return this::asString;
    }

    @Override
    public ConvertorToBytes<String> getConvertorToBytes() {
        return this::toBytesBuffer;
    }

    @Override
    public VarLengthWriter<String> getTypeWriter() {
        return new VarLengthWriter<String>(getConvertorToBytes());
    }

    @Override
    public VarLengthReader<String> getTypeReader() {
        return new VarLengthReader<String>(getConvertorFromBytes());
    }

    @Override
    public Comparator<String> getComparator() {
        return (s1, s2) -> s1.compareTo(s2);
    }

    @Override
    public String getTombstone() {
        return TOMBSTONE_VALUE;
    }

    private String asString(final ByteSequence bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        return new String(bytes.toByteArray(), CHARSET_ENCODING);
    }

    private ByteSequence toBytesBuffer(final String value) {
        Vldtn.requireNonNull(value, "value");
        return Bytes.of(value.getBytes(CHARSET_ENCODING));
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
