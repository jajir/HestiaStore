package org.hestiastore.index.chunkentryfile;

import org.hestiastore.index.IndexException;

/** Built-in, persistently identifiable sorted-key page encodings. */
public final class KeyPageCodecs {
    static final int PREFIX_ID = 2;
    static final int LONG_DELTA_VARINT_ID = 3;
    static final int LONG_FIXED_WEIGHT_DELTA_VARINT_ID = 4;

    private KeyPageCodecs() {
    }

    /**
     * @param <K> key type
     * @return byte-prefix encoding for any descriptor
     */
    public static <K> KeyPageCodec<K> prefix() {
        return new KeyPageCodec<>(PREFIX_ID);
    }

    /**
     * @return first absolute long followed by canonical unsigned LEB128 gaps
     */
    public static KeyPageCodec<Long> longDeltaVarint() {
        return new KeyPageCodec<>(LONG_DELTA_VARINT_ID);
    }

    /**
     * Creates an order-preserving rank codec for non-negative fixed-weight
     * longs in a binary parity class. The first rank occupies eight bytes;
     * following ranks use canonical unsigned LEB128 gaps. Logical keys remain
     * unchanged outside page serialization. Domain parameters are immutable.
     *
     * @param bitCount       logical bit width, from 1 through 63
     * @param setBitCount    required set bits, from zero through bitCount
     * @param parityMasks    ordered parity masks, at most eight, inside
     *                       bitCount; an empty array selects the entire
     *                       fixed-weight domain
     * @param paritySyndrome equation result bits in parityMasks order
     * @return matched primitive-long rank writer and reader selection
     * @throws IllegalArgumentException for invalid parameters or empty domain
     */
    public static KeyPageCodec<Long> longFixedWeightDeltaVarint(
            final int bitCount, final int setBitCount, final long[] parityMasks,
            final int paritySyndrome) {
        return new KeyPageCodec<>(LONG_FIXED_WEIGHT_DELTA_VARINT_ID,
                new LongFixedWeightRank(bitCount, setBitCount, parityMasks,
                        paritySyndrome));
    }

    /**
     * Resolves a persisted built-in format. The consuming reader must validate
     * the logical descriptor before using it.
     *
     * @param <K> expected logical key type
     * @param id  persisted codec identifier
     * @return selected codec
     */
    public static <K> KeyPageCodec<K> fromId(final int id) {
        if (id == LONG_FIXED_WEIGHT_DELTA_VARINT_ID) {
            throw new IndexException(
                    "Fixed-weight key pages require persisted domain parameters.");
        }
        if (id != PREFIX_ID && id != LONG_DELTA_VARINT_ID) {
            throw new IndexException("Unsupported key page codec " + id
                    + "; rebuild indexes written with an older Senku format.");
        }
        return new KeyPageCodec<>(id);
    }
}
