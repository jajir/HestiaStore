package com.coroptis.index.log;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.datatype.ConvertorFromBytes;
import com.coroptis.index.datatype.ConvertorToBytes;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeReader;
import com.coroptis.index.datatype.TypeWriter;

public class TypeDescriptorLoggedKey<K> implements TypeDescriptor<LoggedKey<K>> {

    private final TypeDescriptorLogOperation TDLO = new TypeDescriptorLogOperation();

    private final TypeDescriptor<K> tdKey;

    public TypeDescriptorLoggedKey(final TypeDescriptor<K> tdKey) {
        this.tdKey = Objects.requireNonNull(tdKey);
    }

    @Override
    public ConvertorToBytes<LoggedKey<K>> getConvertorToBytes() {
        return b -> {
            final byte[] f1 = TDLO.getConvertorToBytes().toBytes(b.getLogOperation());
            final byte[] f2 = tdKey.getConvertorToBytes().toBytes(b.getKey());
            final byte[] out = new byte[1 + f2.length];
            out[0] = f1[0];
            System.arraycopy(f2, 0, out, 1, f2.length);
            return out;
        };
    }

    @Override
    public ConvertorFromBytes<LoggedKey<K>> getConvertorFromBytes() {
        return bytes -> {
            final byte[] f2 = new byte[bytes.length - 1];
            System.arraycopy(bytes, 1, f2, 0, bytes.length - 1);
            return LoggedKey.of(LogOperation.fromByte(bytes[0]), tdKey.getConvertorFromBytes().fromBytes(f2));
        };
    }

    @Override
    public TypeReader<LoggedKey<K>> getTypeReader() {
        return inputStream -> {
            return LoggedKey.of(TDLO.getTypeReader().read(inputStream), tdKey.getTypeReader().read(inputStream));
        };
    }

    @Override
    public TypeWriter<LoggedKey<K>> getTypeWriter() {
        return (fileWriter, b) -> {
            return TDLO.getTypeWriter().write(fileWriter, b.getLogOperation())
                    + tdKey.getTypeWriter().write(fileWriter, b.getKey());
        };
    }

    @Override
    public Comparator<LoggedKey<K>> getComparator() {
        return (i1, i2) -> {
            final int out = tdKey.getComparator().compare(i1.getKey(), i2.getKey());
            if (out == 0) {
                return TDLO.getComparator().compare(i1.getLogOperation(), i2.getLogOperation());
            }
            return out;
        };
    }

    @Override
    public LoggedKey<K> getTombstone() {
        throw new UnsupportedOperationException(
                "Unable to use thombstone value for record type that can't be deleted.");
    }
}
