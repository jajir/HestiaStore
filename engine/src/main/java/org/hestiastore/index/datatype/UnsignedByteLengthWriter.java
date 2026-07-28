package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

/**
 * Writes values as {@code [unsigned byte length][payload bytes]} while
 * retaining a reusable payload buffer.
 *
 * @param <T> encoded value type
 */
final class UnsignedByteLengthWriter<T> implements TypeWriter<T> {

    static final int MAX_PAYLOAD_BYTES = 0xFF;

    private final TypeEncoder<T> convertor;
    private byte[] payloadBytes;

    /**
     * Creates an unsigned-byte-length writer.
     *
     * @param convertor payload encoder
     */
    UnsignedByteLengthWriter(final TypeEncoder<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
        this.payloadBytes = new byte[0];
    }

    /**
     * Encodes and writes one value.
     *
     * @param writer target writer
     * @param object value to encode
     * @return total prefix and payload bytes written
     */
    @Override
    public int write(final FileWriter writer, final T object) {
        final FileWriter validatedWriter = Vldtn.requireNonNull(writer,
                "writer");
        final EncodedBytes encodedPayload = convertor.encode(object,
                payloadBytes);
        final int payloadLength = encodedPayload.getLength();
        if (payloadLength > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException(String.format(
                    "Encoded payload length '%s' exceeds maximum '%s'.",
                    payloadLength, MAX_PAYLOAD_BYTES));
        }
        payloadBytes = encodedPayload.getBytes();
        validatedWriter.write((byte) payloadLength);
        validatedWriter.write(payloadBytes, 0, payloadLength);
        return 1 + payloadLength;
    }
}
