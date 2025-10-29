package org.hestiastore.index.datablockfile;

import org.apache.commons.codec.digest.PureJavaCrc32;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

/**
 * Represents the payload of a data block, encapsulating the byte data and
 * providing functionality to calculate its CRC checksum.
 */
public class DataBlockPayload {

    private final Bytes bytes;

    /**
     * Factory method to create a DataBlockPayload instance from the given
     * bytes.
     *
     * @param bytes The byte data to be encapsulated in the payload.
     * @return A new instance of DataBlockPayload containing the provided bytes.
     */
    public static DataBlockPayload of(final Bytes bytes) {
        return new DataBlockPayload(bytes);
    }

    private DataBlockPayload(final Bytes bytes) {
        this.bytes = Vldtn.requireNonNull(bytes, "bytes");
    }

    /**
     * Retrieves the byte data encapsulated in this payload.
     *
     * @return The byte data of the payload.
     */
    public Bytes getBytes() {
        return bytes;
    }

    /**
     * Calculates the CRC checksum of the byte data in this payload.
     *
     * @return The CRC checksum as a long value.
     */
    public long calculateCrc() {
        final PureJavaCrc32 crc = new PureJavaCrc32();
        crc.update(bytes.toByteArray());
        return crc.getValue();
    }

    @Override
    public boolean equals(final Object o) {
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
