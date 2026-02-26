package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

public class VarLengthWriter<T> implements TypeWriter<T> {

    private static final TypeEncoder<Integer> CONVERTOR_TO_BYTES = new TypeDescriptorInteger()
            .getTypeEncoder();
    private static final int LENGTH_HEADER_BYTES = Integer.BYTES;

    private final TypeEncoder<T> convertor;
    private final byte[] lengthBytes;

    public VarLengthWriter(final TypeEncoder<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
        this.lengthBytes = new byte[LENGTH_HEADER_BYTES];
    }

    @Override
    public int write(final FileWriter writer, final T object) {
        final byte[] out = TypeEncoder.toByteArray(convertor, object);
        CONVERTOR_TO_BYTES.toBytes(out.length, lengthBytes);
        writer.write(lengthBytes);
        writer.write(out);
        return LENGTH_HEADER_BYTES + out.length;
    }
}
