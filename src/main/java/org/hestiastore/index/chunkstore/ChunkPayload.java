package org.hestiastore.index.chunkstore;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequenceCrc32;
import org.hestiastore.index.Vldtn;

/**
 * Represents the payload of a chunk in the chunk store. It encapsulates the
 * byte data and provides methods for accessing the data, calculating its
 * length, and computing its CRC checksum.
 */
public class ChunkPayload {

    private final ByteSequence bytes;

    /**
     * Factory method to create a ChunkPayload instance from the given bytes.
     *
     * @param bytes required byte data for the chunk payload
     * @throws IllegalArgumentException if bytes is null
     */
    public static ChunkPayload of(final ByteSequence bytes) {
        return new ChunkPayload(Vldtn.requireNonNull(bytes, "bytes"));
    }

    private ChunkPayload(final ByteSequence bytes) {
        this.bytes = bytes;
    }

    /**
     * Get the raw bytes of the chunk payload.
     * 
     * @return the raw bytes of the chunk payload
     */
    public ByteSequence getBytes() {
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
        final ByteSequenceCrc32 crc = new ByteSequenceCrc32();
        crc.update(bytes);
        return crc.getValue();
    }

    @Override
    public int hashCode() {
        int result = 1;
        final int length = bytes.length();
        for (int i = 0; i < length; i++) {
            result = 31 * result + bytes.getByte(i);
        }
        return result;
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
        final int length = bytes.length();
        if (length != other.bytes.length()) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (bytes.getByte(i) != other.bytes.getByte(i)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "ChunkPayload{" + "bytes=" + bytes + '}';
    }
}
