package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequenceCrc32;
import org.hestiastore.index.bytes.ByteSequences;

/**
 * Represents the payload of a data block, encapsulating the byte data and
 * providing functionality to calculate its CRC checksum.
 */
public class DataBlockPayload {

    private final ByteSequence bytes;

    /**
     * Factory method to create a payload from byte sequence.
     *
     * @param bytes payload sequence
     * @return payload instance
     */
    public static DataBlockPayload ofSequence(final ByteSequence bytes) {
        return new DataBlockPayload(bytes);
    }

    private DataBlockPayload(final ByteSequence bytes) {
        this.bytes = Vldtn.requireNonNull(bytes, "bytes");
    }

    /**
     * Retrieves the byte sequence encapsulated in this payload.
     *
     * @return payload sequence
     */
    public ByteSequence getBytesSequence() {
        return bytes;
    }

    /**
     * Retrieves payload length in bytes.
     *
     * @return payload length
     */
    public int length() {
        return bytes.length();
    }

    /**
     * Calculates the CRC checksum of the byte data in this payload.
     *
     * @return The CRC checksum as a long value.
     */
    public long calculateCrc() {
        final ByteSequenceCrc32 crc = new ByteSequenceCrc32();
        crc.update(bytes);
        return crc.getValue();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof DataBlockPayload))
            return false;
        DataBlockPayload that = (DataBlockPayload) o;
        return ByteSequences.contentEquals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return ByteSequences.contentHashCode(bytes);
    }

    @Override
    public String toString() {
        return "DataBlockPayload{" + "length=" + bytes.length() + '}';
    }

}
