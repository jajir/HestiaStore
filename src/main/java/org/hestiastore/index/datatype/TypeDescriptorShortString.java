package org.hestiastore.index.datatype;

import java.nio.charset.Charset;
import java.util.Comparator;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

public class TypeDescriptorShortString implements TypeDescriptor<String> {

    private static final String CHARSET_ENCODING_NAME = "ISO_8859_1";

    private static final Charset CHARSET_ENCODING = Charset
            .forName(CHARSET_ENCODING_NAME);

    /**
     * Tombstones value, use can't use it.
     */
    public static final String TOMBSTONE_VALUE = "(*&^%$#@!)-1eaa9b2c-3c11-11ee-be56-0242ac120002";

    @Override
    public ConvertorFromBytes<String> getConvertorFromBytes() {
        return bytes -> {
            Vldtn.requireNonNull(bytes, "bytes");
            return new String(bytes.toByteArray(), CHARSET_ENCODING);
        };
    }

    @Override
    public ConvertorToBytes<String> getConvertorToBytes() {
        return string -> {
            Vldtn.requireNonNull(string, "string");
            return Bytes.of(string.getBytes(CHARSET_ENCODING));
        };
    }

    @Override
    public VarShortLengthWriter<String> getTypeWriter() {
        return new VarShortLengthWriter<String>(getConvertorToBytes());
    }

    @Override
    public VarShortLengthReader<String> getTypeReader() {
        return new VarShortLengthReader<String>(getConvertorFromBytes());
    }

    @Override
    public Comparator<String> getComparator() {
        return (s1, s2) -> s1.compareTo(s2);
    }

    @Override
    public String getTombstone() {
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
