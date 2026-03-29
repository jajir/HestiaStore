package org.hestiastore.index.chunkstore;

/**
 * Interface for processing chunk data.
 */
public interface ChunkFilter {

    int BIT_POSITION_MAGIC_NUMBER = 0;
    int BIT_POSITION_CRC32 = 1;
    int BIT_POSITION_SNAPPY_COMPRESSION = 3;
    int BIT_POSITION_XOR_ENCRYPT = 4;
    int BIT_POSITION_AES_GCM_ENCRYPT = 5;

    /**
     * Apply the filter to the input chunk data.
     *
     * @param input The input chunk data.
     * @return The processed chunk data.
     */
    ChunkData apply(ChunkData input);

}
