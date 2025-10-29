package org.hestiastore.index.datatype;

import java.nio.charset.Charset;
import java.util.Comparator;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.MutableBytes;
import org.hestiastore.index.Vldtn;

public final class TypeDescriptorFixedLengthString
        implements TypeDescriptor<String> {

    private static final String CHARSET_ENCODING_NAME = "ISO_8859_1";

    private static final Charset CHARSET_ENCODING = Charset
            .forName(CHARSET_ENCODING_NAME);

    /**
     * Tombstones value, use can't use it.
     */
    private static final String TOMBSTONE_DEFAULT_VALUE = //
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
        return bytes -> {
            Vldtn.requireNonNull(bytes, "bytes");
            if (length != bytes.length()) {
                throw new IllegalArgumentException(String.format(
                        "Byte array length should be '%s' but is '%s'", length,
                        bytes.length()));
            }
            return new String(bytes.toByteArray(), CHARSET_ENCODING);
        };
    }

    @Override
    public ConvertorToBytes<String> getConvertorToBytes() {
        return string -> {
            Vldtn.requireNonNull(string, "string");
            if (length != string.length()) {
                throw new IllegalArgumentException(String.format(
                        "String length shoudlld be '%s' but is '%s'", length,
                        string.length()));
            }
            return Bytes.of(string.getBytes(CHARSET_ENCODING));
        };
    }

    @Override
    public TypeWriter<String> getTypeWriter() {
        return new FixedLengthWriter<String>(getConvertorToBytes());
    }

    @Override
    public TypeReader<String> getTypeReader() {
        return reader -> {
            final MutableBytes buffer = MutableBytes.allocate(length);
            reader.read(buffer);
            return getConvertorFromBytes().fromBytes(buffer.toBytes());
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
