package org.hestiastore.index.log;

import java.util.Comparator;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.MutableBytes;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.ConvertorFromBytes;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.datatype.TypeWriter;

public class TypeDescriptorLoggedKey<K>
        implements TypeDescriptor<LoggedKey<K>> {

    private static final TypeDescriptorLogOperation TDLO = new TypeDescriptorLogOperation();

    private final TypeDescriptor<K> tdKey;

    public TypeDescriptorLoggedKey(final TypeDescriptor<K> typeDescriptorKey) {
        this.tdKey = Vldtn.requireNonNull(typeDescriptorKey,
                "typeDescriptorKey");
    }

    @Override
    public ConvertorToBytes<LoggedKey<K>> getConvertorToBytes() {
        return loggedKey -> {
            Vldtn.requireNonNull(loggedKey, "loggedKey");
            final ByteSequence operationBytes = TDLO.getConvertorToBytes()
                    .toBytesBuffer(loggedKey.getLogOperation());
            final ByteSequence keyBytes = tdKey.getConvertorToBytes()
                    .toBytesBuffer(loggedKey.getKey());
            if (operationBytes.length() != 1) {
                throw new IllegalStateException(
                        "Log operation encoding must be exactly one byte");
            }
            final MutableBytes out = MutableBytes
                    .allocate(1 + keyBytes.length());
            out.setByte(0, operationBytes.getByte(0));
            out.setBytes(1, keyBytes);
            return out.toByteSequence();
        };
    }

    @Override
    public ConvertorFromBytes<LoggedKey<K>> getConvertorFromBytes() {
        return bytes -> {
            Vldtn.requireNonNull(bytes, "bytes");
            if (bytes.length() < 1) {
                throw new IllegalArgumentException(
                        "LoggedKey encoding must contain at least one byte");
            }
            final byte operation = bytes.getByte(0);
            final ByteSequence keyBytes = bytes.slice(1, bytes.length());
            return LoggedKey.of(LogOperation.fromByte(operation),
                    tdKey.getConvertorFromBytes().fromBytes(keyBytes));
        };
    }

    @Override
    public TypeReader<LoggedKey<K>> getTypeReader() {
        return inputStream -> {
            final LogOperation logOperation = TDLO.getTypeReader()
                    .read(inputStream);
            if (logOperation == null) {
                return null;
            }
            return LoggedKey.of(logOperation,
                    tdKey.getTypeReader().read(inputStream));
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
            final int out = tdKey.getComparator().compare(i1.getKey(),
                    i2.getKey());
            if (out == 0) {
                return TDLO.getComparator().compare(i1.getLogOperation(),
                        i2.getLogOperation());
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
