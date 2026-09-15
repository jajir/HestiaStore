package org.hestiastore.index.chunkstore;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.bytes.ByteSequences;
import com.github.luben.zstd.Zstd;

/**
 * Decodes Zstd chunks or their uncompressed fallback, with bounded allocation.
 */
public final class ChunkFilterZstdDecompress implements ChunkFilter {
    @Override
    public ChunkData apply(final ChunkData input) {
        if ((input.getFlags() & (1L << BIT_POSITION_SNAPPY_COMPRESSION)) != 0) {
            throw new IndexException(
                    "Snappy is not supported by this Senku format; rebuild the index.");
        }
        if ((input.getFlags() & ChunkFilterZstdCompress.FLAG_COMPRESSED) == 0) {
            return input;
        }
        try {
            final byte[] payload = input.getPayloadSequence().toByteArray();
            final long size = Zstd.getFrameContentSize(payload);
            if (size <= 0 || size > ChunkFilterZstdCompress.MAX_PAYLOAD_BYTES) {
                throw new IndexException(
                        "Invalid or oversized Zstd frame content size: "
                                + size);
            }
            final byte[] decoded = Zstd.decompress(payload, (int) size);
            if (decoded.length != size) {
                throw new IndexException(
                        "Zstd decoded length does not match its frame.");
            }
            return input.withPayloadSequence(ByteSequences.wrap(decoded))
                    .withFlags(input.getFlags()
                            & ~ChunkFilterZstdCompress.FLAG_COMPRESSED);
        } catch (Exception e) {
            throw new IndexException("Unable to decode Zstd chunk.", e);
        }
    }
}
