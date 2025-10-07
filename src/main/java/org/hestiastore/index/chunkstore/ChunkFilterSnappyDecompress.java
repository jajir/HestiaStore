package org.hestiastore.index.chunkstore;

import java.io.IOException;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.IndexException;
import org.xerial.snappy.Snappy;

/**
 * Decompresses payload previously compressed with Snappy.
 */
public class ChunkFilterSnappyDecompress implements ChunkFilter {

    private static final long FLAG_COMPRESSED = 1L << 0;

    @Override
    public ChunkData apply(final ChunkData input) {
        if ((input.getFlags() & FLAG_COMPRESSED) == 0) {
            throw new IllegalStateException(
                    "Chunk payload is not marked as Snappy compressed.");
        }
        try {
            final byte[] decompressed = decompressPayload(input.getPayload());
            final Bytes decompressedBytes = Bytes.of(decompressed);
            return input.withPayload(decompressedBytes)
                    .withFlags(input.getFlags() & ~FLAG_COMPRESSED);
        } catch (IOException ex) {
            throw new IndexException("Unable to decompress chunk payload", ex);
        }
    }

    byte[] decompressPayload(final Bytes payload) throws IOException {
        return Snappy.uncompress(payload.getData());
    }
}
