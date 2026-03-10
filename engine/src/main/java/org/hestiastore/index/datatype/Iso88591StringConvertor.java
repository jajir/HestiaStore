package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;

/**
 * Converts strings using ISO-8859-1 and supports writing into reusable buffers.
 */
final class Iso88591StringConvertor implements TypeEncoder<String> {

    static final Iso88591StringConvertor INSTANCE = new Iso88591StringConvertor();
    private static final byte REPLACEMENT_BYTE = (byte) '?';

    private Iso88591StringConvertor() {
    }

    @Override
    public EncodedBytes encode(final String object, final byte[] reusableBuffer) {
        final String validated = Vldtn.requireNonNull(object, "object");
        final byte[] validatedBuffer = Vldtn.requireNonNull(reusableBuffer,
                "reusableBuffer");
        byte[] output = validatedBuffer;
        final int maxRequired = validated.length();
        if (output.length < maxRequired) {
            output = new byte[maxRequired];
        }
        final int written = encodeToBuffer(validated, output);
        return new EncodedBytes(output, written);
    }

    private int encodeToBuffer(final String value, final byte[] destination) {
        final int inputLength = value.length();
        int out = 0;
        for (int i = 0; i < inputLength; i++) {
            final char c = value.charAt(i);
            if (c <= 0x00FF) {
                destination[out++] = (byte) c;
                continue;
            }
            destination[out++] = REPLACEMENT_BYTE;
            if (Character.isHighSurrogate(c) && i + 1 < inputLength
                    && Character.isLowSurrogate(value.charAt(i + 1))) {
                i++;
            }
        }
        return out;
    }
}
