package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

/**
 * Writes fixed-length encoded values using a reusable internal payload buffer.
 *
 * @param <T> value type
 */
public class FixedLengthWriter<T> implements TypeWriter<T> {

    private final TypeEncoder<T> convertor;
    private byte[] payloadBytes;

    /**
     * Creates a fixed-length writer for a given encoder.
     *
     * @param convertor encoder used to serialize values
     */
    public FixedLengthWriter(final TypeEncoder<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
        this.payloadBytes = new byte[0];
    }

    /**
     * Serializes and writes a single value.
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
        writer.write(payloadBytes, 0, payloadLength);
        return payloadLength;
    }

    private void ensurePayloadBufferSize(final int payloadLength) {
        if (payloadBytes.length < payloadLength) {
            payloadBytes = new byte[payloadLength];
        }
    }

}
