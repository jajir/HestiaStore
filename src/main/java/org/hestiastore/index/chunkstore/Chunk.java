package org.hestiastore.index.chunkstore;

import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

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

    private final Bytes bytes;

    /**
     * Create a chunk from raw bytes.
     * 
     * @param bytes the raw bytes of the chunk
     * @return new chunk instance
     */
    public static Chunk of(final ByteSequence bytes) {
        return new Chunk(toBytes(bytes));
    }

    /**
     * Create a chunk from header and payload.
     * 
     * @param header  required chunk header
     * @param payload required chunk payload
     * @return new chunk instance
     * @throws IllegalArgumentException if the chunk is invalid
     */
    public static Chunk of(final ChunkHeader header,
            final ByteSequence payload) {
        Vldtn.requireNonNull(header, "header");
        Vldtn.requireNonNull(payload, "payload");
        final Bytes headerBytes = toBytes(header.getBytes());
        final Bytes payloadBytes = toBytes(payload);
        final Bytes bytes = Bytes.concat(headerBytes, payloadBytes);
        return new Chunk(bytes);
    }

    private Chunk(final Bytes bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        final ChunkHeader header = ChunkHeader
                .of(bytes.subBytes(0, ChunkHeader.HEADER_SIZE));
        final int requiredLength = header.getPayloadLength();
        if (bytes.length() != requiredLength + ChunkHeader.HEADER_SIZE) {
            throw new IllegalArgumentException(String.format(
                    "Chunk bytes length '%s' is not equal to required length '%s'",
                    bytes.length(), requiredLength));
        }
        this.bytes = bytes;
    }

    /**
     * Get the raw bytes of the chunk.
     * 
     * @return the raw bytes of the chunk
     */
    public ByteSequence getBytes() {
        return bytes;
    }

    /**
     * Get the payload of the chunk.
     * 
     * @return the payload of the chunk
     */
    public ChunkPayload getPayload() {
        return ChunkPayload
                .of(bytes.subBytes(ChunkHeader.HEADER_SIZE, bytes.length()));
    }

    /**
     * Get the header of the chunk.
     * 
     * @return the header of the chunk
     */
    public ChunkHeader getHeader() {
        return ChunkHeader.of(bytes.subBytes(0, ChunkHeader.HEADER_SIZE));
    }

    /**
     * Calculate the CRC of the chunk payload.
     * 
     * @return the CRC of the chunk payload
     */
    public long calculateCrc() {
        return getPayload().calculateCrc();
    }

    private static Bytes toBytes(final ByteSequence sequence) {
        if (sequence instanceof Bytes) {
            return (Bytes) sequence;
        }
        return Bytes.copyOf(sequence);
    }
}
