package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

/**
 * Writes values as {@code [unsigned LEB128 byte length][payload bytes]} while
 * retaining a reusable payload buffer.
 *
 * @param <T> encoded value type
 */
final class VarIntLengthWriter<T> implements TypeWriter<T> {

    private final TypeEncoder<T> convertor;
    private byte[] payloadBytes;

    /**
     * Creates a variable-length writer.
     *
     * @param convertor payload encoder
     */
    VarIntLengthWriter(final TypeEncoder<T> convertor) {
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
        if (payloadLength > UnsignedLeb128.MAX_FRAME_PAYLOAD_BYTES) {
            throw new IllegalArgumentException(String.format(
                    "Encoded payload length '%s' exceeds maximum '%s'.",
                    payloadLength, UnsignedLeb128.MAX_FRAME_PAYLOAD_BYTES));
        }
        payloadBytes = encodedPayload.getBytes();
        final int prefixLength = UnsignedLeb128.write(validatedWriter,
                payloadLength);
        validatedWriter.write(payloadBytes, 0, payloadLength);
        return prefixLength + payloadLength;
    }
}
