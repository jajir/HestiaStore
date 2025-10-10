package org.hestiastore.index.chunkstore;

import java.io.IOException;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.IndexException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

/**
 * Compresses the chunk payload using Snappy and marks the chunk as compressed.
 */
public class ChunkFilterSnappyCompress implements ChunkFilter {

    static final long FLAG_COMPRESSED = 1L << BIT_POSITION_SNAPPY_COMPRESSION;
    private static final Logger logger = LoggerFactory
            .getLogger(ChunkFilterSnappyCompress.class);

    @Override
    public ChunkData apply(final ChunkData input) {
        try {
            final Bytes payload = input.getPayload();
            final int originalSize = payload.length();
            final byte[] compressed = compressPayload(payload);
            final int compressedSize = compressed.length;
            if (logger.isDebugEnabled() && originalSize > 0) {
                final double savings = 100.0
                        * (originalSize - compressedSize) / originalSize;
                logger.debug(
                        "Snappy compressed chunk: {} -> {} bytes (savings: {}%)",
                        originalSize, compressedSize,
                        String.format("%.2f", savings));
            }
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
