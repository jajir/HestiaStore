package org.hestiastore.index.chunkstore;

/**
 * Writes the canonical chunk magic number into metadata.
 */
public class ChunkFilterMagicNumberWriting implements ChunkFilter {

    static final long FLAG_MASK = 1L << BIT_POSITION_MAGIC_NUMBER;

    @Override
    public ChunkData apply(final ChunkData input) {
        return input.withMagicNumber(ChunkHeader.MAGIC_NUMBER)
                .withFlags(input.getFlags() | FLAG_MASK);
    }
}
