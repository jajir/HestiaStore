package org.hestiastore.index.blockdatafile;

import org.apache.commons.codec.digest.PureJavaCrc32;
import org.hestiastore.index.Bytes;

public class DataBlockPayload {

    private final Bytes bytes;

    public static DataBlockPayload of(final Bytes bytes) {
        return new DataBlockPayload(bytes);
    }

    DataBlockPayload(final Bytes bytes) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof DataBlockPayload))
            return false;
        DataBlockPayload that = (DataBlockPayload) o;
        return bytes.equals(that.bytes);
    }

    @Override
    public int hashCode() {
        return bytes.hashCode();
    }

    @Override
    public String toString() {
        return "DataBlockPayload{" + "bytes=" + bytes + '}';
    }

}
