package org.hestiastore.index.chunkstore;

/**
 * Writes the canonical chunk magic number into metadata.
 */
public class ChunkFilterMagicNumberWriting implements ChunkFilter {

    @Override
    public ChunkData apply(final ChunkData input) {
        return input.withMagicNumber(ChunkHeader.MAGIC_NUMBER);
    }
}
