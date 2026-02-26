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
    public TypeDecoder<NullValue> getTypeDecoder() {
        return byters -> NULL;
    }

    @Override
    public TypeEncoder<NullValue> getTypeEncoder() {
        return new TypeEncoder<NullValue>() {
            @Override
            public int bytesLength(final NullValue value) {
                return 0;
            }

            @Override
            public int toBytes(final NullValue value, final byte[] destination) {
                return 0;
            }
        };
    }

    @Override
    public NullValue getTombstone() {
        return TOMBSTONE;
    }

}
