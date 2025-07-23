package org.hestiastore.index.blockdatafile;

import org.hestiastore.index.Vldtn;

/**
 * Block of data bytes.
 */
public class DataBytes {

    private final byte[] data;

    public static DataBytes of(final byte[] data) {
        return new DataBytes(data);
    }

    private DataBytes(final byte[] data) {
        this.data = Vldtn.requireNonNull(data, "data");
    }

    public byte[] getData() {
        return data;
    }

    public int length() {
        return data.length;
    }

}
