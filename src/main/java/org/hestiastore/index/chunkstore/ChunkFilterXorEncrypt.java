package org.hestiastore.index.chunkstore;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequenceView;
import org.hestiastore.index.bytes.ByteSequences;

/**
 * Applies a reversible XOR transformation over the payload bytes.
 */
public class ChunkFilterXorEncrypt implements ChunkFilter {

    static final long FLAG_ENCRYPTED = 1L << BIT_POSITION_XOR_ENCRYPT;
    private static final long DEFAULT_KEY = 0x9E3779B97F4A7C15L;

    @Override
    public ChunkData apply(final ChunkData input) {
        final ByteSequence transformed = xorPayload(input.getPayload());
        return input.withPayload(transformed)
                .withFlags(input.getFlags() | FLAG_ENCRYPTED);
    }

    static ByteSequence xorPayload(final ByteSequence payload) {
        final int length = payload.length();
        final byte[] target = new byte[length];
        for (int i = 0; i < length; i++) {
            final int shift = (i % Long.BYTES) * 8;
            final int keyByte = (int) ((DEFAULT_KEY >>> shift) & 0xFF);
            target[i] = (byte) (payload.getByte(i) ^ keyByte);
        }
        return ByteSequences.wrap(target);
    }
}
