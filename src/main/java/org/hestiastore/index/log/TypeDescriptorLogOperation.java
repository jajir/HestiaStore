package org.hestiastore.index.log;

import java.util.Comparator;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.ConvertorFromBytes;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorByte;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.datatype.TypeWriter;

public class TypeDescriptorLogOperation
        implements TypeDescriptor<LogOperation> {

    private static final byte END_OF_FILE = -1;

    private static final TypeDescriptorByte TDB = new TypeDescriptorByte();

    @Override
    public ConvertorToBytes<LogOperation> getConvertorToBytes() {
        return operation -> {
            Vldtn.requireNonNull(operation, "operation");
            return TDB.getConvertorToBytes().toBytesBuffer(operation.getByte());
        };
    }

    @Override
    public ConvertorFromBytes<LogOperation> getConvertorFromBytes() {
        return bytes -> {
            Vldtn.requireNonNull(bytes, "bytes");
            if (bytes.length() != 1) {
                throw new IllegalArgumentException(
                        "LogOperation requires exactly one byte");
            }
            return LogOperation.fromByte(bytes.getData()[0]);
        };
    }

    @Override
    public TypeReader<LogOperation> getTypeReader() {
        return inputStream -> {
            byte b = (byte) inputStream.read();
            if (b == END_OF_FILE) {
                return null;
            }
            return LogOperation.fromByte(b);
        };
    }

    @Override
    public TypeWriter<LogOperation> getTypeWriter() {
        return (fileWriter, b) -> {
            fileWriter.write(b.getByte());
            return 1;
        };
    }

    @Override
    public Comparator<LogOperation> getComparator() {
        return (i1, i2) -> i2.getByte() - i1.getByte();
    }

    @Override
    public LogOperation getTombstone() {
        throw new UnsupportedOperationException(
                "Unable to use thombstone value for record type that can't be deleted.");
    }

}
