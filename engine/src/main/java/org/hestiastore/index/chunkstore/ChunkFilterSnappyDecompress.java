package org.hestiastore.index.chunkstore;

import java.io.IOException;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.xerial.snappy.Snappy;

/**
 * Decompresses payload previously compressed with Snappy.
 */
public class ChunkFilterSnappyDecompress implements ChunkFilter {
    @Override
    public ChunkData apply(final ChunkData input) {
        if ((input.getFlags()
                & ChunkFilterSnappyCompress.FLAG_COMPRESSED) == 0) {
            throw new IllegalStateException(
                    "Chunk payload is not marked as Snappy compressed.");
        }
        try {
            final ByteSequence payload = input.getPayloadSequence();
            final byte[] decompressed = decompressPayload(payload);
            return input.withPayloadSequence(ByteSequences.wrap(decompressed))
                    .withFlags(input.getFlags()
                            & ~ChunkFilterSnappyCompress.FLAG_COMPRESSED);
        } catch (IOException ex) {
            throw new IndexException("Unable to decompress chunk payload", ex);
        }
    }

    byte[] decompressPayload(final ByteSequence payload) throws IOException {
        return Snappy.uncompress(payload.toByteArray());
    }
}
