package org.hestiastore.index.datatype;

import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

public class VarLengthWriter<T> implements TypeWriter<T> {

    private static final ConvertorToBytes<Integer> CONVERTOR_TO_BYTES = new TypeDescriptorInteger()
            .getConvertorToBytes();

    private final ConvertorToBytes<T> convertor;

    public VarLengthWriter(final ConvertorToBytes<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
    }

    @Override
    public int write(final FileWriter writer, final T object) {
        final ByteSequence out = convertor.toBytesBuffer(object);
        if (out.length() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Converted type is too big");
        }
        final ByteSequence lengthBytes = CONVERTOR_TO_BYTES
                .toBytesBuffer(out.length());
        writer.write(lengthBytes);
        writer.write(out);
        return lengthBytes.length() + out.length();
    }
}
