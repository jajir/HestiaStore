package com.coroptis.index.datatype;

import java.nio.charset.Charset;
import java.util.Comparator;

public class TypeDescriptorFixedLengthString implements TypeDescriptor<String> {

    private final static String CHARSET_ENCODING_NAME = "ISO_8859_1";

    private final static Charset CHARSET_ENCODING = Charset
            .forName(CHARSET_ENCODING_NAME);

    /**
     * Tombstones value, use can't use it.
     */
    private final static String TOMBSTONE_DEFAULT_VALUE = //
            ""//
                    + "(*&^%$#@!)-1eaa9b2c-"//
                    + "3c11-11ee-be56-0242a"//
                    + "c120002-n8328nx§b8 §"//
                    + "utf1l1098g76231uy979"//
                    + "c120002-n8328nx§b8 §"//
                    + "utf1l1098g76231uy979"//
                    + "8e2313gj";

    private final int length;
    private final String tombstone;

    public TypeDescriptorFixedLengthString(final int length) {
        if (length >= 128) {
            throw new IllegalArgumentException(
                    "Max fixed length string is 127 characters.");
        }
        this.length = length;
        tombstone = TOMBSTONE_DEFAULT_VALUE.substring(0, length);
    }

    @Override
    public ConvertorFromBytes<String> getConvertorFromBytes() {
        return array -> {
            if (length != array.length) {
                throw new IllegalArgumentException(String.format(
                        "Byte array length should be '%s' but is '%s'", length,
                        array.length));
            }
            return new String(array, CHARSET_ENCODING);
        };
    }

    @Override
    public ConvertorToBytes<String> getConvertorToBytes() {
        return string -> {
            if (length != string.length()) {
                throw new IllegalArgumentException(String.format(
                        "String length shoudlld be '%s' but is '%s'", length,
                        string.length()));
            }
            return string.getBytes(CHARSET_ENCODING);
        };
    }

    @Override
    public TypeWriter<String> getTypeWriter() {
        return new FixedLengthWriter<String>(getConvertorToBytes());
    }

    @Override
    public TypeReader<String> getTypeReader() {
        return reader -> {
            final byte[] in = new byte[length];
            reader.read(in);
            return getConvertorFromBytes().fromBytes(in);
        };
    }

    @Override
    public Comparator<String> getComparator() {
        return (s1, s2) -> s1.compareTo(s2);
    }

    @Override
    public String getTombstone() {
        return tombstone;
    }

}
