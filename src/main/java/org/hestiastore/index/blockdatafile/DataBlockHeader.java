package org.hestiastore.index.blockdatafile;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.datatype.ConvertorFromBytes;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorLong;

public class DataBlockHeader {

    private static final TypeDescriptor<Long> TYPE_DESCRIPTOR_LONG = new TypeDescriptorLong();
    private static final ConvertorFromBytes<Long> CONVERTOR_FROM_BYTES = TYPE_DESCRIPTOR_LONG
            .getConvertorFromBytes();

    private final Bytes bytes;

    public DataBlockHeader(final Bytes bytes) {
        this.bytes = bytes;
    }

    public long getMagicNumber() {
        final byte[] buff = bytes.subBytes(0, 8).getData();
        return CONVERTOR_FROM_BYTES.fromBytes(buff);
    }

    public long getCrc() {
        final byte[] buff = bytes.subBytes(8, 8).getData();
        return CONVERTOR_FROM_BYTES.fromBytes(buff);
    }

}
