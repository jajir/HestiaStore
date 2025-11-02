package org.hestiastore.index.datablockfile;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequenceCrc32;
import org.hestiastore.index.bytes.Bytes;
import org.hestiastore.index.bytes.MutableBytes;
import org.hestiastore.index.Vldtn;

/**
 * Represents the payload of a data block, encapsulating the byte data and
 * providing functionality to calculate its CRC checksum.
 */
public class DataBlockPayload {

    private final ByteSequence bytes;

    /**
     * Factory method to create a DataBlockPayload instance from the given
     * bytes.
     *
     * @param bytes The byte data to be encapsulated in the payload.
     * @return A new instance of DataBlockPayload containing the provided bytes.
     */
    public static DataBlockPayload of(final ByteSequence bytes) {
        return new DataBlockPayload(normalize(bytes));
    }

    private DataBlockPayload(final ByteSequence bytes) {
        this.bytes = normalize(bytes);
    }

    /**
     * Retrieves the byte data encapsulated in this payload.
     *
     * @return The byte data of the payload.
     */
    public ByteSequence getBytes() {
        return bytes;
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
        return contentEquals(bytes, that.bytes);
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
    public String toString() {
        return "DataBlockPayload{" + "bytes=" + bytes + '}';
    }
    private static boolean contentEquals(final ByteSequence first,
            final ByteSequence second) {
        if (first == second) {
            return true;
        }
        final int length = first.length();
        if (length != second.length()) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (first.getByte(i) != second.getByte(i)) {
                return false;
            }
        }
        return true;
    }

    private static ByteSequence normalize(final ByteSequence sequence) {
        final ByteSequence validated = Vldtn.requireNonNull(sequence, "bytes");
        if (validated.isEmpty()) {
            return Bytes.EMPTY;
        }
        if (validated instanceof Bytes) {
            return validated;
        }
        if (validated instanceof MutableBytes) {
            return ((MutableBytes) validated).toImmutableBytes();
        }
        return validated.slice(0, validated.length());
    }
}
