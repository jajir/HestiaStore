package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;

/**
 * Converts strings using ISO-8859-1 and supports writing into reusable buffers.
 */
@SuppressWarnings("java:S6548")
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
        int index = 0;
        while (index < inputLength) {
            final char c = value.charAt(index);
            if (c <= 0x00FF) {
                destination[out++] = (byte) c;
                index++;
                continue;
            }
            destination[out++] = REPLACEMENT_BYTE;
            index += consumesSurrogatePair(value, index, c) ? 2 : 1;
        }
        return out;
    }

    private static boolean consumesSurrogatePair(final String value,
            final int index, final char currentChar) {
        return Character.isHighSurrogate(currentChar) && index + 1 < value.length()
                && Character.isLowSurrogate(value.charAt(index + 1));
    }
}
