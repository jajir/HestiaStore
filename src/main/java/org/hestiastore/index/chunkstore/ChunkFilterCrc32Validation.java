package org.hestiastore.index.chunkstore;

import org.apache.commons.codec.digest.PureJavaCrc32;

/**
 * Validates that the stored CRC32 matches the current payload bytes.
 */
public class ChunkFilterCrc32Validation implements ChunkFilter {

    @Override
    public ChunkData apply(final ChunkData input) {
        final PureJavaCrc32 crc = new PureJavaCrc32();
        crc.update(input.getPayload().getData());
        final long calculated = crc.getValue();
        if (calculated != input.getCrc()) {
            throw new IllegalStateException(String.format(
                    "Invalid CRC32. Expected '%s' but calculated '%s'",
                    input.getCrc(), calculated));
        }
        return input;
    }
}
