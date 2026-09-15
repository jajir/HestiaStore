package org.hestiastore.index.chunkentryfile;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

/** Page-local primitive encoding state for sorted long keys. */
final class LongKeyPageWriter {
    private final boolean delta;
    private final KeyPageCodec<?> codec;
    private final byte[] buffer = new byte[Long.BYTES];
    private boolean hasPrevious;
    private long previous;

    LongKeyPageWriter(final KeyPageCodec<?> codec) {
        this.codec = Vldtn.requireNonNull(codec, "keyPageCodec");
        delta = codec.isLongDeltaVarint();
    }

    /** Writes a key without boxing or allocating a per-key byte array. */
    void write(final FileWriter writer, final long logicalKey) {
        final long key = codec.encodeLongKey(logicalKey);
        writeKey(writer, key);
    }

    /**
     * Transfers an already encoded key while checking its full source domain
     * and range. The encoded value is not ranked a second time.
     */
    void writeEncoded(final FileWriter writer, final long encodedKey,
            final KeyPageCodec<?> sourceCodec) {
        Vldtn.requireTrue(codec.hasSameEncoding(sourceCodec),
                "Encoded key source and target codec domains differ");
        codec.validateEncodedLongKey(encodedKey);
        writeKey(writer, encodedKey);
    }

    private void writeKey(final FileWriter writer, final long key) {
        if (hasPrevious && key <= previous) {
            throw new IllegalArgumentException(
                    "Long keys must be strictly increasing.");
        }
        if (delta && hasPrevious) {
            long gap = key - previous;
            while ((gap & ~0x7fL) != 0) {
                writer.write((byte) ((gap & 0x7f) | 0x80));
                gap >>>= 7;
            }
            writer.write((byte) gap);
        } else {
            final int shared = hasPrevious
                    ? Long.numberOfLeadingZeros(previous ^ key) / Byte.SIZE
                    : 0;
            if (!delta) {
                writer.write((byte) shared);
                writer.write((byte) (Long.BYTES - shared));
            }
            for (int i = shared; i < Long.BYTES; i++) {
                buffer[i] = (byte) (key >>> ((7 - i) * Byte.SIZE));
            }
            writer.write(buffer, shared, Long.BYTES - shared);
        }
        previous = key;
        hasPrevious = true;
    }
}
