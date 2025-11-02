package org.hestiastore.index.chunkstore;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ConcatenatedByteSequence;
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

    private final ByteSequence bytes;

    /**
     * Create a chunk from raw bytes.
     * 
     * @param bytes the raw bytes of the chunk
     * @return new chunk instance
     */
    public static Chunk of(final ByteSequence bytes) {
        final ByteSequence checked = Vldtn.requireNonNull(bytes, "bytes");
        return new Chunk(checked);
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
        final ByteSequence payloadBytes = Vldtn.requireNonNull(payload,
                "payload");
        final int declaredLength = header.getPayloadLength();
        if (payloadBytes.length() != declaredLength) {
            throw new IllegalArgumentException(String.format(
                    "Payload size '%d' does not match header declared payload length '%d'",
                    payloadBytes.length(), declaredLength));
        }
        final ByteSequence headerBytes = header.getBytes();
        return new Chunk(
                ConcatenatedByteSequence.of(headerBytes, payloadBytes));
    }

    private Chunk(final ByteSequence bytes) {
        this.bytes = Vldtn.requireNonNull(bytes, "bytes");
        if (bytes.length() < ChunkHeader.HEADER_SIZE) {
            throw new IllegalArgumentException(String.format(
                    "Chunk bytes length '%s' is smaller than required header size '%s'",
                    bytes.length(), ChunkHeader.HEADER_SIZE));
        }
        final ChunkHeader header = ChunkHeader
                .of(bytes.slice(0, ChunkHeader.HEADER_SIZE));
        final int requiredLength = header.getPayloadLength();
        if (bytes.length() != requiredLength + ChunkHeader.HEADER_SIZE) {
            throw new IllegalArgumentException(String.format(
                    "Chunk bytes length '%s' is not equal to required length '%s'",
                    bytes.length(), requiredLength));
        }
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
                .of(bytes.slice(ChunkHeader.HEADER_SIZE, bytes.length()));
    }

    /**
     * Get the header of the chunk.
     * 
     * @return the header of the chunk
     */
    public ChunkHeader getHeader() {
        return ChunkHeader.of(bytes.slice(0, ChunkHeader.HEADER_SIZE));
    }

    /**
     * Calculate the CRC of the chunk payload.
     * 
     * @return the CRC of the chunk payload
     */
    public long calculateCrc() {
        return getPayload().calculateCrc();
    }
}
