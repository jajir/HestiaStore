package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequenceCrc32;
import org.hestiastore.index.bytes.ByteSequences;

/**
 * Represents the payload of a chunk in the chunk store. It encapsulates the
 * byte data and provides methods for accessing the data, calculating its
 * length, and computing its CRC checksum.
 */
public class ChunkPayload {

    private final ByteSequence bytes;

    /**
     * Factory method to create a ChunkPayload from a byte sequence.
     *
     * @param bytes required byte sequence
     * @return created chunk payload
     */
    public static ChunkPayload ofSequence(final ByteSequence bytes) {
        return new ChunkPayload(bytes);
    }

    private ChunkPayload(final ByteSequence bytes) {
        this.bytes = Vldtn.requireNonNull(bytes, "bytes");
    }

    /**
     * Get the raw bytes of the chunk payload as sequence.
     *
     * @return payload byte sequence
     */
    public ByteSequence getBytesSequence() {
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
        return ByteSequences.contentHashCode(bytes);
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
        return ByteSequences.contentEquals(bytes, other.bytes);
    }

    @Override
    public String toString() {
        return "ChunkPayload{" + "length=" + bytes.length() + '}';
    }
}
