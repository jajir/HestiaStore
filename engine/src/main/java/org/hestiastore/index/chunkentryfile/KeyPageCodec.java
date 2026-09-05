package org.hestiastore.index.chunkentryfile;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.sorteddatafile.DiffKeyReader;

/**
 * Immutable selection of a matched sorted-key page writer and reader. Codec
 * instances may be shared; each page creates fresh reader/writer state.
 * Supported formats are obtained from {@link KeyPageCodecs}.
 *
 * @param <K> logical key type
 */
public final class KeyPageCodec<K> {
    private final int id;
    private final LongFixedWeightRank fixedWeightRank;

    KeyPageCodec(final int id) {
        this(id, null);
    }

    KeyPageCodec(final int id, final LongFixedWeightRank fixedWeightRank) {
        this.id = id;
        this.fixedWeightRank = fixedWeightRank;
    }

    /** @return stable persisted codec identifier */
    public int getId() {
        return id;
    }

    /** @return whether keys are stored as unsigned numeric delta-varints */
    public boolean isLongDeltaVarint() {
        return id == KeyPageCodecs.LONG_DELTA_VARINT_ID
                || isLongFixedWeightDeltaVarint();
    }

    /** @return whether numeric deltas encode fixed-weight parity-class ranks */
    public boolean isLongFixedWeightDeltaVarint() {
        return id == KeyPageCodecs.LONG_FIXED_WEIGHT_DELTA_VARINT_ID;
    }

    /** @return logical bit width of this fixed-weight codec */
    public int getFixedWeightBitCount() {
        return requireFixedWeightRank().bitCount();
    }

    /** @return required number of set bits in this fixed-weight codec */
    public int getFixedWeightSetBitCount() {
        return requireFixedWeightRank().setBitCount();
    }

    /** @return defensive copy of the ordered binary parity equation masks */
    public long[] getFixedWeightParityMasks() {
        return requireFixedWeightRank().parityMasks();
    }

    /** @return required ordered parity-equation result bits */
    public int getFixedWeightParitySyndrome() {
        return requireFixedWeightRank().paritySyndrome();
    }

    /** Maps a logical primitive key only at the page serialization boundary. */
    long encodeLongKey(final long key) {
        return fixedWeightRank == null ? key : fixedWeightRank.rank(key);
    }

    /**
     * Restores the logical primitive key before it reaches a merge or caller.
     */
    long decodeLongKey(final long encodedKey) {
        return fixedWeightRank == null ? encodedKey
                : fixedWeightRank.unrank(encodedKey);
    }

    private LongFixedWeightRank requireFixedWeightRank() {
        if (fixedWeightRank == null) {
            throw new IndexException("Codec has no fixed-weight parameters.");
        }
        return fixedWeightRank;
    }

    /**
     * Checks that the descriptor implements the selected ordering and encoding.
     * Delta encoding supports exact built-in signed-long ordering, including
     * differences crossing zero; it does not reinterpret custom comparators.
     *
     * @param descriptor logical key descriptor
     */
    public void validate(final TypeDescriptor<?> descriptor) {
        Vldtn.requireNonNull(descriptor, "keyTypeDescriptor");
        if (isLongDeltaVarint()
                && descriptor.getClass() != TypeDescriptorLong.class) {
            throw new IndexException(
                    "Long delta-varint pages require TypeDescriptorLong.");
        }
    }

    /**
     * Creates fresh key decoding state for one page. Values continue to use
     * their own descriptor and share the caller's interleaved byte stream.
     *
     * @param descriptor logical key descriptor
     * @return page-local key reader
     */
    @SuppressWarnings("unchecked")
    public TypeReader<K> createReader(final TypeDescriptor<K> descriptor) {
        validate(descriptor);
        if (descriptor.getClass() == TypeDescriptorLong.class) {
            return (TypeReader<K>) new LongKeyPageReader(this);
        }
        return new DiffKeyReader<>(descriptor.getTypeDecoder());
    }
}
