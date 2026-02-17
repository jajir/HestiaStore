package org.hestiastore.index.datatype;

import static org.hestiastore.index.datatype.NullValue.NULL;
import static org.hestiastore.index.datatype.NullValue.TOMBSTONE;

import java.util.Comparator;

/**
 * Greeat advantage of null value is that it occupied no space in the index.
 */
public class TypeDescriptorNull implements TypeDescriptor<NullValue> {

    @Override
    public Comparator<NullValue> getComparator() {
        return (o1, o2) -> {
            // Null values are considered equal
            return 0;
        };
    }

    @Override
    public TypeReader<NullValue> getTypeReader() {
        return fileReader -> {
            return NULL;
        };
    }

    @Override
    public TypeWriter<NullValue> getTypeWriter() {
        return (fileWriter, object) -> {
            return 0;
        };
    }

    @Override
    public ConvertorFromBytes<NullValue> getConvertorFromBytes() {
        return byters -> NULL;
    }

    @Override
    public ConvertorToBytes<NullValue> getConvertorToBytes() {
        // NullValue is represented by an empty byte array
        return value -> new byte[0];
    }

    @Override
    public NullValue getTombstone() {
        return TOMBSTONE;
    }

}
