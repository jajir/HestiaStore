package com.coroptis.index.type;

import java.nio.charset.Charset;
import java.util.Comparator;

public class TypeDescriptorString implements TypeDescriptor<String> {

    private final static String CHARSET_ENCODING_NAME = "ISO_8859_1";

    private final static Charset CHARSET_ENCODING = Charset.forName(CHARSET_ENCODING_NAME);

    @Override
    public ConvertorFromBytes<String> getConvertorFromBytes() {
        return array -> new String(array, CHARSET_ENCODING);
    }

    @Override
    public ConvertorToBytes<String> getConvertorToBytes() {
        return string -> string.getBytes(CHARSET_ENCODING);
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
}
