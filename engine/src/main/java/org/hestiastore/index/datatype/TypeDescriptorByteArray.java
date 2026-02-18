package org.hestiastore.index.datatype;

import java.nio.charset.Charset;
import java.util.Comparator;

public class TypeDescriptorByteArray implements TypeDescriptor<ByteArray> {

    private static final String CHARSET_ENCODING_NAME = "ISO_8859_1";

    private static final Charset CHARSET_ENCODING = Charset
            .forName(CHARSET_ENCODING_NAME);

    /**
     * Tombstones value, use can't use it.
     */
    public static final ByteArray TOMBSTONE_VALUE = ByteArray
            .of("(*&^%$#@!)-1eaa9b2c-3c11-11ee-be56-0242ac120002"
                    .getBytes(CHARSET_ENCODING));

    @Override
    public ConvertorFromBytes<ByteArray> getConvertorFromBytes() {
        return array -> ByteArray.of(array);
    }

    @Override
    public ConvertorToBytes<ByteArray> getConvertorToBytes() {
        return byteArray -> byteArray.getBytes();
    }

    @Override
    public VarLengthWriter<ByteArray> getTypeWriter() {
        return new VarLengthWriter<ByteArray>(getConvertorToBytes());
    }

    @Override
    public VarLengthReader<ByteArray> getTypeReader() {
        return new VarLengthReader<ByteArray>(getConvertorFromBytes());
    }

    @Override
    public Comparator<ByteArray> getComparator() {
        return (s1, s2) -> s1.compareTo(s2);
    }

    @Override
    public ByteArray getTombstone() {
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
