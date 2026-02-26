package org.hestiastore.index.datatype;

import java.nio.charset.Charset;

/**
 * Converts strings using ISO-8859-1 and supports writing into reusable buffers.
 */
final class Iso88591StringConvertor implements ConvertorToBytes<String> {

    static final Iso88591StringConvertor INSTANCE = new Iso88591StringConvertor();
    private static final Charset CHARSET_ENCODING = Charset
            .forName("ISO_8859_1");
    private static final byte REPLACEMENT_BYTE = (byte) '?';

    private Iso88591StringConvertor() {
    }

    @Override
    public byte[] toBytes(final String object) {
        return object.getBytes(CHARSET_ENCODING);
    }

    @Override
    public int bytesLength(final String object) {
        int out = 0;
        for (int i = 0; i < object.length(); i++) {
            final char c = object.charAt(i);
            if (c <= 0x00FF) {
                out++;
                continue;
            }
            out++;
            if (Character.isHighSurrogate(c) && i + 1 < object.length()
                    && Character.isLowSurrogate(object.charAt(i + 1))) {
                i++;
            }
        }
        return out;
    }

    @Override
    public int toBytes(final String object, final byte[] destination) {
        final int required = bytesLength(object);
        if (destination.length < required) {
            throw new IllegalArgumentException(String.format(
                    "Destination buffer too small. Required '%s' but was '%s'",
                    required, destination.length));
        }
        int out = 0;
        for (int i = 0; i < object.length(); i++) {
            final char c = object.charAt(i);
            if (c <= 0x00FF) {
                destination[out++] = (byte) c;
                continue;
            }
            destination[out++] = REPLACEMENT_BYTE;
            if (Character.isHighSurrogate(c) && i + 1 < object.length()
                    && Character.isLowSurrogate(object.charAt(i + 1))) {
                i++;
            }
        }
        return out;
    }
}
