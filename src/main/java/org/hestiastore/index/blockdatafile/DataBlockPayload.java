package org.hestiastore.index.blockdatafile;

import org.hestiastore.index.Bytes;

public class DataBlockPayload {

    Bytes bytes;

    public DataBlockPayload(final Bytes bytes) {
        this.bytes = bytes;
    }

    public Bytes getBytes() {
        return bytes;
    }

}
