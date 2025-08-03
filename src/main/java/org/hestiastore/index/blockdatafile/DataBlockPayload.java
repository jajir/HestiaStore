package org.hestiastore.index.blockdatafile;

import org.apache.commons.codec.digest.PureJavaCrc32;
import org.hestiastore.index.Bytes;

public class DataBlockPayload {

    Bytes bytes;

    public DataBlockPayload(final Bytes bytes) {
        this.bytes = bytes;
    }

    public Bytes getBytes() {
        return bytes;
    }

    public long calculateCrc() {
        final PureJavaCrc32 crc = new PureJavaCrc32();
        crc.update(bytes.getData());
        return crc.getValue();
    }

}
