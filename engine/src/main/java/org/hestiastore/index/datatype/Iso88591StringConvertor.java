package org.hestiastore.index.datatype;

/**
 * Converts strings using ISO-8859-1 and supports writing into reusable buffers.
 */
final class Iso88591StringConvertor implements TypeEncoder<String> {

    static final Iso88591StringConvertor INSTANCE = new Iso88591StringConvertor();
    private static final byte REPLACEMENT_BYTE = (byte) '?';

    private Iso88591StringConvertor() {
    }

    /**
     * Returns encoded size in bytes for ISO-8859-1 representation.
     *
     * @param object source string
     * @return encoded byte length
     */
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

    /**
     * Encodes string into destination buffer using ISO-8859-1 compatible
     * mapping.
     *
     * @param object      source string
     * @param destination destination byte buffer
     * @return number of bytes written
     */
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
