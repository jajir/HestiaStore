package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Bytes;

/**
 * Applies a reversible XOR transformation over the payload bytes.
 */
public class ChunkFilterXorEncrypt implements ChunkFilter {

    static final long FLAG_ENCRYPTED = 1L << 1;
    private static final long DEFAULT_KEY = 0x9E3779B97F4A7C15L;

    @Override
    public ChunkData apply(final ChunkData input) {
        final Bytes transformed = xorPayload(input.getPayload());
        return input.withPayload(transformed)
                .withFlags(input.getFlags() | FLAG_ENCRYPTED);
    }

    static Bytes xorPayload(final Bytes payload) {
        final byte[] source = payload.getData();
        final byte[] target = new byte[source.length];
        for (int i = 0; i < source.length; i++) {
            final int shift = (i % Long.BYTES) * 8;
            final int keyByte = (int) ((DEFAULT_KEY >>> shift) & 0xFF);
            target[i] = (byte) (source[i] ^ keyByte);
        }
        return Bytes.of(target);
    }
}
