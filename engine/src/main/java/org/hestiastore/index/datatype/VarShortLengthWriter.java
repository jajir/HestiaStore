package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

/**
 * Writes values encoded as {@code [1-byte length][payload bytes]} using a
 * reusable internal payload buffer.
 *
 * @param <T> encoded value type
 */
public class VarShortLengthWriter<T> implements TypeWriter<T> {

    private final TypeEncoder<T> convertor;
    private byte[] payloadBytes;

    /**
     * Creates a short-length writer.
     *
     * @param convertor payload encoder
     */
    public VarShortLengthWriter(final TypeEncoder<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
        this.payloadBytes = new byte[0];
    }

    /**
     * Writes one value as {@code [length][payload]} where length is one byte.
     *
     * @param writer target writer
     * @param object value to encode
     * @return number of bytes written
     */
    @Override
    public int write(final FileWriter writer, final T object) {
        final int payloadLength = Vldtn.requireGreaterThanOrEqualToZero(
                convertor.bytesLength(object), "payloadLength");
        if (payloadLength > 127) {
            throw new IllegalArgumentException("Converted type is too big");
        }
        ensurePayloadBufferSize(payloadLength);
        final int writtenBytes = convertor.toBytes(object, payloadBytes);
        if (writtenBytes != payloadLength) {
            throw new IllegalStateException(String.format(
                    "Encoder wrote '%s' bytes but declared '%s'", writtenBytes,
                    payloadLength));
        }
        writer.write((byte) payloadLength);
        writer.write(payloadBytes, 0, payloadLength);
        return 1 + payloadLength;
    }

    private void ensurePayloadBufferSize(final int payloadLength) {
        if (payloadBytes.length < payloadLength) {
            payloadBytes = new byte[payloadLength];
        }
    }

}
