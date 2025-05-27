package org.hestiastore.index.log;

import java.util.Comparator;

import org.hestiastore.index.datatype.ConvertorFromBytes;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorByte;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.datatype.TypeWriter;

public class TypeDescriptorLogOperation
        implements TypeDescriptor<LogOperation> {

    private final static byte END_OF_FILE = -1;

    private final static TypeDescriptorByte TDB = new TypeDescriptorByte();

    @Override
    public ConvertorToBytes<LogOperation> getConvertorToBytes() {
        return b -> {
            return TDB.getConvertorToBytes().toBytes(b.getByte());
        };
    }

    @Override
    public ConvertorFromBytes<LogOperation> getConvertorFromBytes() {
        return bytes -> LogOperation.fromByte(bytes[0]);
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
