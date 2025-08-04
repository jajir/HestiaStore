package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

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
     * Version of the chunk format. It is used to identify the format of the
     * chunk and ensure compatibility between different versions of the chunk
     * format.
     * 
     * real version is remotely related to main library version when it's
     * introduced.
     */
    static final int VERSION = 0xff_00_00_05;

    private final Bytes bytes;

    public static Chunk of(final Bytes bytes) {
        return new Chunk(bytes);
    }

    private Chunk(final Bytes bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        final ChunkHeader header = getHeader();
        final int requiredLength = header.getPayloadLength() + HEADER_SIZE;
        if (bytes.length() != requiredLength) {
            throw new IllegalArgumentException(String.format(
                    "Chunk bytes length is not equal to required length '%s'",
                    requiredLength));
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

}
