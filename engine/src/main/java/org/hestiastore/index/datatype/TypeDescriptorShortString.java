package org.hestiastore.index.datatype;

import java.nio.charset.Charset;
import java.util.Comparator;

/**
 * Descriptor for short strings prefixed by one-byte payload length.
 */
public class TypeDescriptorShortString implements TypeDescriptor<String> {

    private static final String CHARSET_ENCODING_NAME = "ISO_8859_1";

    private static final Charset CHARSET_ENCODING = Charset
            .forName(CHARSET_ENCODING_NAME);

    private static final TypeEncoder<String> CONVERTOR_TO_BYTES = Iso88591StringConvertor.INSTANCE;

    /**
     * Tombstone value reserved for delete semantics.
     */
    public static final String TOMBSTONE_VALUE = "(*&^%$#@!)-1eaa9b2c-3c11-11ee-be56-0242ac120002";

    /**
     * Returns decoder for ISO-8859-1 strings.
     *
     * @return decoder
     */
    @Override
    public TypeDecoder<String> getTypeDecoder() {
        return array -> new String(array, CHARSET_ENCODING);
    }

    /**
     * Returns encoder for ISO-8859-1 strings.
     *
     * @return encoder
     */
    @Override
    public TypeEncoder<String> getTypeEncoder() {
        return CONVERTOR_TO_BYTES;
    }

    /**
     * Returns short-length writer for strings.
     *
     * @return writer
     */
    @Override
    public VarShortLengthWriter<String> getTypeWriter() {
        return new VarShortLengthWriter<String>(getTypeEncoder());
    }

    /**
     * Returns short-length reader for strings.
     *
     * @return reader
     */
    @Override
    public VarShortLengthReader<String> getTypeReader() {
        return new VarShortLengthReader<String>(getTypeDecoder());
    }

    /**
     * Returns natural-order comparator.
     *
     * @return comparator
     */
    @Override
    public Comparator<String> getComparator() {
        return (s1, s2) -> s1.compareTo(s2);
    }

    /**
     * Returns tombstone marker.
     *
     * @return tombstone value
     */
    @Override
    public String getTombstone() {
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
