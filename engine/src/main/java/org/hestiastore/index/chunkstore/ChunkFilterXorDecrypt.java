package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Bytes;

/**
 * Reverses {@link ChunkFilterXorEncrypt} transformation.
 */
public class ChunkFilterXorDecrypt implements ChunkFilter {

    @Override
    public ChunkData apply(final ChunkData input) {
        if ((input.getFlags() & ChunkFilterXorEncrypt.FLAG_ENCRYPTED) == 0) {
            throw new IllegalStateException(
                    "Chunk payload is not marked as encrypted.");
        }
        final Bytes restored = ChunkFilterXorEncrypt
                .xorPayload(input.getPayload());
        return input.withPayload(restored).withFlags(
                input.getFlags() & ~ChunkFilterXorEncrypt.FLAG_ENCRYPTED);
    }
}
