package org.hestiastore.index.chunkstore;

/**
 * Validates that the chunk magic number matches the expected constant.
 */
public class ChunkFilterMagicNumberValidation implements ChunkFilter {

    @Override
    public ChunkData apply(final ChunkData input) {
        if ((input.getFlags() & ChunkFilterMagicNumberWriting.FLAG_MASK) == 0) {
            throw new IllegalStateException(
                    "Chunk payload is not marked as containing magic number.");
        }
        if (input.getMagicNumber() != ChunkHeader.MAGIC_NUMBER) {
            throw new IllegalStateException(String.format(
                    "Invalid chunk magic number. Expected '%s' but was '%s'",
                    ChunkHeader.MAGIC_NUMBER, input.getMagicNumber()));
        }
        return input;
    }
}
