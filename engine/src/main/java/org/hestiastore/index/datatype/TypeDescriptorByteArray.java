package org.hestiastore.index.datatype;

import java.nio.charset.Charset;
import java.util.Comparator;

/**
 * Descriptor for {@link ByteArray} values.
 */
public class TypeDescriptorByteArray implements TypeDescriptor<ByteArray> {

    private static final String CHARSET_ENCODING_NAME = "ISO_8859_1";

    private static final Charset CHARSET_ENCODING = Charset
            .forName(CHARSET_ENCODING_NAME);

    /**
     * Tombstone value reserved for delete semantics.
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

    /**
     * Returns decoder for {@link ByteArray}.
     *
     * @return decoder
     */
    @Override
    public TypeDecoder<ByteArray> getTypeDecoder() {
        return array -> ByteArray.of(array);
    }

    /**
     * Returns encoder for {@link ByteArray}.
     *
     * @return encoder
     */
    @Override
    public TypeEncoder<ByteArray> getTypeEncoder() {
        return CONVERTOR_TO_BYTES;
    }

    /**
     * Returns variable-length writer for byte arrays.
     *
     * @return writer
     */
    @Override
    public VarLengthWriter<ByteArray> getTypeWriter() {
        return new VarLengthWriter<ByteArray>(getTypeEncoder());
    }

    /**
     * Returns variable-length reader for byte arrays.
     *
     * @return reader
     */
    @Override
    public VarLengthReader<ByteArray> getTypeReader() {
        return new VarLengthReader<ByteArray>(getTypeDecoder());
    }

    /**
     * Returns lexicographic comparator over unsigned bytes.
     *
     * @return comparator
     */
    @Override
    public Comparator<ByteArray> getComparator() {
        return (s1, s2) -> s1.compareTo(s2);
    }

    /**
     * Returns tombstone marker.
     *
     * @return tombstone value
     */
    @Override
    public ByteArray getTombstone() {
        return TOMBSTONE_VALUE;
    }

    /**
     * Returns whether object is the same descriptor type.
     *
     * @param obj object to compare
     * @return {@code true} when same descriptor type
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        return obj != null && getClass() == obj.getClass();
    }

    /**
     * Returns stable hash code for descriptor type identity.
     *
     * @return hash code
     */
    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

}
