package org.hestiastore.index.chunkstore;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.bytes.ByteSequences;
import com.github.luben.zstd.Zstd;

/** Stateless Zstd encoder with an uncompressed fallback for expanding pages. */
public final class ChunkFilterZstdCompress implements ChunkFilter {
    static final long FLAG_COMPRESSED = 1L << BIT_POSITION_ZSTD_COMPRESSION;
    static final int MAX_PAYLOAD_BYTES = 256 * 1024 * 1024;
    private final int level;

    /** @param level supported Zstd compression level */
    public ChunkFilterZstdCompress(final int level) {
        this.level = Compression.zstd(level).getLevel();
    }

    @Override
    public ChunkData apply(final ChunkData input) {
        if ((input.getFlags() & (FLAG_COMPRESSED
                | (1L << BIT_POSITION_SNAPPY_COMPRESSION))) != 0) {
            throw new IndexException(
                    "Zstd input is already marked as compressed.");
        }
        if (input.getPayloadSequence().length() > MAX_PAYLOAD_BYTES) {
            throw new IndexException(
                    "Zstd chunk exceeds the 256 MiB decoded page limit.");
        }
        final byte[] payload = input.getPayloadSequence().toByteArray();
        try {
            final byte[] compressed = Zstd.compress(payload, level);
            if (compressed.length >= payload.length) {
                return input.withFlags(input.getFlags() & ~FLAG_COMPRESSED);
            }
            return input.withPayloadSequence(ByteSequences.wrap(compressed))
                    .withFlags(input.getFlags() | FLAG_COMPRESSED);
        } catch (Exception e) {
            throw new IndexException("Unable to compress Zstd chunk.", e);
        }
    }
}
