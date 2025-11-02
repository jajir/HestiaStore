package org.hestiastore.index.chunkstore;

import org.hestiastore.index.bytes.ByteSequenceCrc32;

/**
 * Computes CRC32 for the current payload and stores it in the chunk metadata.
 */
public class ChunkFilterCrc32Writing implements ChunkFilter {

    @Override
    public ChunkData apply(final ChunkData input) {
        final ByteSequenceCrc32 crc = new ByteSequenceCrc32();
        crc.update(input.getPayload());
        return input.withCrc(crc.getValue());
    }
}
