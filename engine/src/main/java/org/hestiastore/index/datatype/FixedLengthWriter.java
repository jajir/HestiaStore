package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

public class FixedLengthWriter<T> implements TypeWriter<T> {

    private final TypeEncoder<T> convertor;
    private byte[] payloadBytes;

    public FixedLengthWriter(final TypeEncoder<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
        this.payloadBytes = new byte[0];
    }

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
        writer.write(payloadBytes);
        return payloadLength;
    }

    private void ensurePayloadBufferSize(final int payloadLength) {
        if (payloadBytes.length != payloadLength) {
            payloadBytes = new byte[payloadLength];
        }
    }

}
