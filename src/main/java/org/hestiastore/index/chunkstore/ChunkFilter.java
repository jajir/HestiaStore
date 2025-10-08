package org.hestiastore.index.chunkstore;

/**
 * Interface for processing chunk data.
 */
public interface ChunkFilter {

    public static final int BIT_POSITION_MAGIC_NUMBER = 0;
    public static final int BIT_POSITION_CRC32 = 1;
    public static final int BIT_POSITION_SNAPPY_COMPRESSION = 3;
    public static final int BIT_POSITION_XOR_ENCRYPT = 4;

    /**
     * Apply the filter to the input chunk data.
     *
     * @param input The input chunk data.
     * @return The processed chunk data.
     */
    ChunkData apply(ChunkData input);

}
