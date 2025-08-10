package org.hestiastore.index.chunkstore;

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

    static final int HEADER_SIZE = 32;

    /**
     * Size of cell in bytes. Cell is smalles addresable unit in chunk store.
     */
    static final int CELL_SIZE = 16;

    /**
     * "theodora" in ASCII
     */
    static final long MAGIC_NUMBER = 0x7468656F646F7261L;

    /**
     * Uncompressed chunk format version 1
     */
    static final int VERSION_01_00 = 0xff_00_01_00;

    /**
     * Compressed chunk format version 2
     */
    static final int VERSION_02_00 = 0xff_00_02_00;

    private final Bytes bytes;

    public static Chunk of(final Bytes bytes) {
        return new Chunk(bytes);
    }

    public static Chunk of(final ChunkHeader header, final Bytes payload) {
        Vldtn.requireNonNull(header, "header");
        Vldtn.requireNonNull(payload, "payload");
        final Bytes bytes = Bytes.of(header.getBytes(), payload);
        return new Chunk(bytes);
    }

    private Chunk(final Bytes bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        final ChunkHeader header = ChunkHeader
                .of(bytes.subBytes(0, HEADER_SIZE));
        final int requiredLength = header.getPayloadLength();
        if (bytes.length() != requiredLength + HEADER_SIZE) {
            throw new IllegalArgumentException(String.format(
                    "Chunk bytes length '%s' is not equal to required length '%s'",
                    bytes.length(), requiredLength));
        }
        if (header.getMagicNumber() != MAGIC_NUMBER) {
            throw new IllegalArgumentException(
                    "Invalid magic number in chunk header");
        }
        this.bytes = bytes;
    }

    public Bytes getBytes() {
        return bytes;
    }

    public ChunkPayload getPayload() {
        return new ChunkPayload(bytes.subBytes(HEADER_SIZE, bytes.length()));
    }

    public ChunkHeader getHeader() {
        return ChunkHeader.of(bytes.subBytes(0, HEADER_SIZE));
    }

    void validate() {
        final ChunkHeader header = getHeader();
        if (header.getMagicNumber() != MAGIC_NUMBER) {
            throw new IllegalArgumentException(
                    "Invalid magic number in chunk header");
        }
        if (header.getCrc() != calculateCrc()) {
            throw new IllegalArgumentException("CRC mismatch in chunk header");
        }
    }

    public long calculateCrc() {
        return getPayload().calculateCrc();
    }
}
