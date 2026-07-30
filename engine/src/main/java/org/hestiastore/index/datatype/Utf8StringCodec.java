package org.hestiastore.index.datatype;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;

/**
 * Strict UTF-8 encoder and decoder that rejects malformed input and reuses
 * caller-provided encoding buffers.
 */
@SuppressWarnings("java:S6548")
final class Utf8StringCodec
        implements TypeEncoder<String>, TypeDecoder<String> {

    static final Utf8StringCodec INSTANCE = new Utf8StringCodec();

    private static final int MIN_GROWTH_BYTES = 32;

    private Utf8StringCodec() {
    }

    /**
     * Encodes a Java string as strict standard UTF-8.
     *
     * @param value string to encode
     * @param reusableBuffer caller-provided reusable buffer
     * @return encoded payload and effective byte length
     */
    @Override
    public EncodedBytes encode(final String value,
            final byte[] reusableBuffer) {
        final String validatedValue = Vldtn.requireNonNull(value, "value");
        final byte[] validatedBuffer = Vldtn.requireNonNull(reusableBuffer,
                "reusableBuffer");
        final CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        final CharBuffer input = CharBuffer.wrap(validatedValue);
        byte[] output = validatedBuffer;
        int written = 0;

        while (true) {
            final ByteBuffer destination = ByteBuffer.wrap(output);
            destination.position(written);
            final CoderResult result = encoder.encode(input, destination, true);
            written = destination.position();
            if (result.isOverflow()) {
                output = grow(output, written, input.remaining());
            } else if (result.isError()) {
                throw encodingException(result);
            } else {
                break;
            }
        }

        while (true) {
            final ByteBuffer destination = ByteBuffer.wrap(output);
            destination.position(written);
            final CoderResult result = encoder.flush(destination);
            written = destination.position();
            if (result.isOverflow()) {
                output = grow(output, written, 0);
            } else if (result.isError()) {
                throw encodingException(result);
            } else {
                return new EncodedBytes(output, written);
            }
        }
    }

    /**
     * Decodes strict standard UTF-8.
     *
     * @param source encoded payload
     * @return decoded Java string
     */
    @Override
    public String decode(final byte[] source) {
        final byte[] validatedSource = Vldtn.requireNonNull(source, "source");
        final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            return decoder.decode(ByteBuffer.wrap(validatedSource)).toString();
        } catch (CharacterCodingException e) {
            throw new IndexException("Invalid UTF-8 payload.", e);
        }
    }

    private static byte[] grow(final byte[] current, final int written,
            final int remainingCharacters) {
        final long doubledLength = Math.max(MIN_GROWTH_BYTES,
                (long) current.length * 2);
        final long estimatedLength = (long) written
                + Math.max(Character.BYTES * 2, remainingCharacters);
        final long requiredLength = Math.max(doubledLength, estimatedLength);
        if (requiredLength > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "UTF-8 payload exceeds the supported array size.");
        }
        return Arrays.copyOf(current, (int) requiredLength);
    }

    private static IllegalArgumentException encodingException(
            final CoderResult result) {
        try {
            result.throwException();
        } catch (CharacterCodingException e) {
            return new IllegalArgumentException(
                    "String contains malformed UTF-16 input.", e);
        }
        return new IllegalArgumentException("Unable to encode string as UTF-8.");
    }
}
