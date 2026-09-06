package org.hestiastore.index.chunkentryfile;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.directory.FileReader;

/**
 * Page-local decoding state shared by generic reads and primitive merges. Full
 * signed-long order is supported; unsigned gaps may occupy ten bytes. Readers
 * borrow their byte source and never close it.
 */
public final class LongKeyPageReader implements TypeReader<Long> {
    private final boolean delta;
    private final KeyPageCodec<?> codec;
    private boolean hasPrevious;
    private long previous;

    /** @param codec selected built-in long encoding */
    public LongKeyPageReader(final KeyPageCodec<?> codec) {
        this.codec = Vldtn.requireNonNull(codec, "keyPageCodec");
        delta = codec.isLongDeltaVarint();
    }

    @Override
    public Long read(final FileReader reader) {
        final int first = reader.read();
        return first < 0 ? null : codec.decodeLongKey(decode(reader, first));
    }

    /**
     * Reads a required primitive key; premature EOF is a corrupt page.
     *
     * @param reader borrowed page byte source
     * @return decoded key, without boxing
     */
    public long readLong(final FileReader reader) {
        return codec.decodeLongKey(readEncodedLong(reader));
    }

    /**
     * Reads a required numeric key or fixed-weight rank without reconstructing
     * the logical key. All framing, strict-order and rank-domain checks remain
     * enabled. Callers must transfer this value only between matching complete
     * codec domains and decode it before exposing a logical key.
     *
     * @param reader borrowed page byte source
     * @return validated encoded primitive key
     */
    public long readEncodedLong(final FileReader reader) {
        return decode(reader, requiredByte(reader));
    }

    private long decode(final FileReader reader, final int first) {
        final long key;
        if (delta) {
            key = hasPrevious ? previous + readGap(reader, first)
                    : readSuffix(reader, first, Long.BYTES);
        } else {
            final int shared = first;
            final int length = requiredByte(reader);
            if (shared > 7 || length < 1 || shared + length != Long.BYTES
                    || (!hasPrevious && shared != 0)) {
                throw new IndexException(
                        "Invalid primitive-long prefix header.");
            }
            final long suffix = readSuffix(reader, requiredByte(reader),
                    length);
            key = length == Long.BYTES ? suffix
                    : (previous & (-1L << (length * Byte.SIZE))) | suffix;
        }
        if (hasPrevious && key <= previous) {
            throw new IndexException(
                    "Decoded long keys are not strictly increasing.");
        }
        codec.validateEncodedLongKey(key);
        previous = key;
        hasPrevious = true;
        return key;
    }

    private static long readGap(final FileReader reader, final int first) {
        long gap = 0;
        int current = first;
        for (int shift = 0; shift <= 63; shift += 7) {
            if (shift == 63 && current > 1) {
                throw new IndexException(
                        "Delta-varint exceeds unsigned 64 bits.");
            }
            gap |= (long) (current & 0x7f) << shift;
            if ((current & 0x80) == 0) {
                if (gap == 0 || (shift > 0 && current == 0)) {
                    throw new IndexException(
                            "Zero or non-canonical delta-varint.");
                }
                return gap;
            }
            current = requiredByte(reader);
        }
        throw new IndexException("Unterminated delta-varint.");
    }

    private static long readSuffix(final FileReader reader, final int first,
            final int length) {
        long value = first;
        for (int i = 1; i < length; i++) {
            value = (value << Byte.SIZE) | requiredByte(reader);
        }
        return value;
    }

    private static int requiredByte(final FileReader reader) {
        final int value = reader.read();
        if (value < 0) {
            throw new IndexException("Truncated long key page.");
        }
        return value;
    }
}
