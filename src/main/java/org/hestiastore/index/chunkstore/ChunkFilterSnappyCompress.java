package org.hestiastore.index.chunkstore;

import java.io.IOException;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.IndexException;
import org.xerial.snappy.Snappy;

/**
 * Compresses the chunk payload using Snappy and marks the chunk as compressed.
 */
public class ChunkFilterSnappyCompress implements ChunkFilter {

    private static final long FLAG_COMPRESSED = 1L << 0;

    @Override
    public ChunkData apply(final ChunkData input) {
        try {
            final byte[] compressed = compressPayload(input.getPayload());
            final Bytes compressedBytes = Bytes.of(compressed);
            return input.withPayload(compressedBytes)
                    .withFlags(input.getFlags() | FLAG_COMPRESSED);
        } catch (IOException ex) {
            throw new IndexException("Unable to compress chunk payload", ex);
        }
    }

    byte[] compressPayload(final Bytes payload) throws IOException {
        return Snappy.compress(payload.getData());
    }
}
