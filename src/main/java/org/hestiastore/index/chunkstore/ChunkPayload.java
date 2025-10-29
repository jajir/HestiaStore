package org.hestiastore.index.chunkstore;

import org.apache.commons.codec.digest.PureJavaCrc32;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

/**
 * Represents the payload of a chunk in the chunk store. It encapsulates the
 * byte data and provides methods for accessing the data, calculating its
 * length, and computing its CRC checksum.
 */
public class ChunkPayload {

    private final Bytes bytes;

    /**
     * Factory method to create a ChunkPayload instance from the given bytes.
     *
     * @param bytes required byte data for the chunk payload
     * @throws IllegalArgumentException if bytes is null
     */
    public static ChunkPayload of(final Bytes bytes) {
        return new ChunkPayload(bytes);
    }

    private ChunkPayload(final Bytes bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        this.bytes = bytes;
    }

    /**
     * Get the raw bytes of the chunk payload.
     * 
     * @return the raw bytes of the chunk payload
     */
    public Bytes getBytes() {
        return bytes;
    }

    /**
     * Get the length of the chunk payload in bytes.
     * 
     * @return the length of the chunk payload in bytes
     */
    public int length() {
        return bytes.length();
    }

    /**
     * Calculate the CRC of the chunk payload.
     * 
     * @return the CRC of the chunk payload
     */
    public long calculateCrc() {
        final PureJavaCrc32 crc = new PureJavaCrc32();
        crc.update(bytes.toByteArray());
        return crc.getValue();
    }

    @Override
    public int hashCode() {
        return bytes.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ChunkPayload)) {
            return false;
        }
        final ChunkPayload other = (ChunkPayload) obj;
        return bytes.equals(other.bytes);
    }

    @Override
    public String toString() {
        return "ChunkPayload{" + "bytes=" + bytes + '}';
    }
}
