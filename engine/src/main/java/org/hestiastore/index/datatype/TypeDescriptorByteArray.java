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

    private static final TypeEncoder<ByteArray> CONVERTOR_TO_BYTES = new TypeEncoder<ByteArray>() {
        @Override
        public int bytesLength(final ByteArray object) {
            return object.length();
        }

        @Override
        public int toBytes(final ByteArray object, final byte[] destination) {
            return object.copyTo(destination);
        }
    };

    @Override
    public TypeDecoder<ByteArray> getTypeDecoder() {
        return array -> ByteArray.of(array);
    }

    @Override
    public TypeEncoder<ByteArray> getTypeEncoder() {
        return CONVERTOR_TO_BYTES;
    }

    @Override
    public VarLengthWriter<ByteArray> getTypeWriter() {
        return new VarLengthWriter<ByteArray>(getTypeEncoder());
    }

    @Override
    public VarLengthReader<ByteArray> getTypeReader() {
        return new VarLengthReader<ByteArray>(getTypeDecoder());
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
