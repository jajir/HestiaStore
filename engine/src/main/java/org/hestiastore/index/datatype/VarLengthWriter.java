package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

/**
 * Writes values encoded as {@code [int length][payload bytes]} using reusable
 * internal buffers.
 *
 * @param <T> encoded value type
 */
public class VarLengthWriter<T> implements TypeWriter<T> {

    private static final TypeEncoder<Integer> CONVERTOR_TO_BYTES = new TypeDescriptorInteger()
            .getTypeEncoder();
    private static final int LENGTH_HEADER_BYTES = Integer.BYTES;

    private final TypeEncoder<T> convertor;
    private final byte[] lengthBytes;
    private byte[] payloadBytes;

    /**
     * Creates a variable-length writer.
     *
     * @param convertor payload encoder
     */
    public VarLengthWriter(final TypeEncoder<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
        this.lengthBytes = new byte[LENGTH_HEADER_BYTES];
        this.payloadBytes = new byte[0];
    }

    /**
     * Writes one value as {@code [length][payload]}.
     *
     * @param writer target writer
     * @param object value to encode
     * @return number of bytes written
     */
    @Override
    public int write(final FileWriter writer, final T object) {
        final int payloadLength = Vldtn.requireGreaterThanOrEqualToZero(
                convertor.bytesLength(object), "payloadLength");
        ensurePayloadBufferSize(payloadLength);
        final int writtenBytes = convertor.toBytes(object, payloadBytes);
        if (writtenBytes != payloadLength) {
            throw new IllegalStateException(String.format(
                    "Encoder wrote '%s' bytes but declared '%s'", writtenBytes,
                    payloadLength));
        }
        CONVERTOR_TO_BYTES.toBytes(payloadLength, lengthBytes);
        writer.write(lengthBytes);
        writer.write(payloadBytes, 0, payloadLength);
        return LENGTH_HEADER_BYTES + payloadLength;
    }

    private void ensurePayloadBufferSize(final int payloadLength) {
        if (payloadBytes.length < payloadLength) {
            payloadBytes = new byte[payloadLength];
        }
    }
}
