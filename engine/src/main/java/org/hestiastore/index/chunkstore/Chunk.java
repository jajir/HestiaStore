package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequenceCrc32;
import org.hestiastore.index.bytes.ConcatenatedByteSequence;

/**
 * Chunk is a data structure that represents a block of data in the chunk store.
 * Chunks are addressable.
 * 
 * 
 * Version of the chunk format. It is used to identify the format of the chunk
 * and ensure compatibility between different versions of the chunk format.
 * 
 * Real version is remotely related to main library version when it's
 * introduced. And include information about compression or encryption.
 * 
 */
public final class Chunk {
    /**
     * Uncompressed chunk format version 1
     */
    static final int VERSION_01_00 = 0xff_00_01_00;

    /**
     * Compressed chunk format version 2
     */
    static final int VERSION_02_00 = 0xff_00_02_00;

    private final ByteSequence bytes;

    /**
     * Create a chunk from raw bytes sequence.
     *
     * @param bytes required byte sequence
     * @return new chunk instance
     */
    public static Chunk ofSequence(final ByteSequence bytes) {
        return new Chunk(bytes);
    }

    /**
     * Create a chunk from header and payload sequence.
     *
     * @param header  required chunk header
     * @param payload required chunk payload
     * @return new chunk instance
     */
    public static Chunk of(final ChunkHeader header, final ByteSequence payload) {
        final ChunkHeader validatedHeader = Vldtn.requireNonNull(header, "header");
        final ByteSequence validatedPayload = Vldtn.requireNonNull(payload,
                "payload");
        final ByteSequence out = ConcatenatedByteSequence.of(
                validatedHeader.getBytesSequence(),
                validatedPayload);
        return new Chunk(out);
    }

    private Chunk(final ByteSequence bytes) {
        this.bytes = Vldtn.requireNonNull(bytes, "bytes");
        final ChunkHeader header = getHeader();
        final int requiredLength = header.getPayloadLength();
        if (bytes.length() != requiredLength + ChunkHeader.HEADER_SIZE) {
            throw new IllegalArgumentException(String.format(
                    "Chunk bytes length '%s' is not equal to required length '%s'",
                    bytes.length(), requiredLength));
        }
    }

    /**
     * Get the raw bytes of the chunk as sequence.
     *
     * @return raw bytes sequence
     */
    public ByteSequence getBytesSequence() {
        return bytes;
    }

    /**
     * Get the payload bytes of the chunk as sequence.
     *
     * @return payload bytes sequence
     */
    public ByteSequence getPayloadSequence() {
        return bytes.slice(ChunkHeader.HEADER_SIZE, bytes.length());
    }

    /**
     * Get the header of the chunk.
     * 
     * @return the header of the chunk
     */
    public ChunkHeader getHeader() {
        return ChunkHeader.ofSequence(bytes.slice(0, ChunkHeader.HEADER_SIZE));
    }

    /**
     * Calculate the CRC of the chunk payload.
     * 
     * @return the CRC of the chunk payload
     */
    public long calculateCrc() {
        final ByteSequenceCrc32 crc = new ByteSequenceCrc32();
        crc.update(getPayloadSequence());
        return crc.getValue();
    }
}
