package org.hestiastore.index.chunkstore;

import org.hestiastore.index.bytes.ByteSequenceCrc32;

/**
 * Validates that the stored CRC32 matches the current payload bytes.
 */
public class ChunkFilterCrc32Validation implements ChunkFilter {

    @Override
    public ChunkData apply(final ChunkData input) {
        final ByteSequenceCrc32 crc = new ByteSequenceCrc32();
        crc.update(input.getPayloadSequence());
        final long calculated = crc.getValue();
        if (calculated != input.getCrc()) {
            throw new IllegalStateException(String.format(
                    "Invalid CRC32. Expected '%s' but calculated '%s'",
                    input.getCrc(), calculated));
        }
        return input;
    }
}
