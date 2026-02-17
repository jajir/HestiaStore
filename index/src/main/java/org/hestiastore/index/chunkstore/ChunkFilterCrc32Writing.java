package org.hestiastore.index.chunkstore;

import org.apache.commons.codec.digest.PureJavaCrc32;

/**
 * Computes CRC32 for the current payload and stores it in the chunk metadata.
 */
public class ChunkFilterCrc32Writing implements ChunkFilter {

    @Override
    public ChunkData apply(final ChunkData input) {
        final PureJavaCrc32 crc = new PureJavaCrc32();
        crc.update(input.getPayload().getData());
        return input.withCrc(crc.getValue());
    }
}
