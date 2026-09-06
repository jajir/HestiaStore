package org.hestiastore.index.chunkentryfile;

import java.util.Arrays;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;

/**
 * Immutable order-preserving rank of non-negative fixed-weight bit strings in a
 * specified binary parity class. Dynamic-programming counts are shared by every
 * reader and writer using the codec; per-key operations allocate nothing.
 */
final class LongFixedWeightRank {

    private final int bitCount;
    private final int setBitCount;
    private final long[] parityMasks;
    private final int paritySyndrome;
    private final long allowedBits;
    private final int syndromeCount;
    private final int weightStride;
    private final int[] bitSyndromes;
    private final long[] counts;
    private final long domainSize;

    /** Builds a bounded domain with at most eight parity equations. */
    LongFixedWeightRank(final int bitCount, final int setBitCount,
            final long[] parityMasks, final int paritySyndrome) {
        this.bitCount = Vldtn.requireBetween(bitCount, 1, 63, "bitCount");
        this.setBitCount = Vldtn.requireBetween(setBitCount, 0, bitCount,
                "setBitCount");
        final long[] validatedMasks = Vldtn.requireNonNull(parityMasks,
                "parityMasks");
        Vldtn.requireBetween(validatedMasks.length, 0, 8, "parityMaskCount");
        this.parityMasks = validatedMasks.clone();
        syndromeCount = 1 << validatedMasks.length;
        this.paritySyndrome = Vldtn.requireBetween(paritySyndrome, 0,
                syndromeCount - 1, "paritySyndrome");
        allowedBits = -1L >>> (Long.SIZE - bitCount);
        for (final long mask : this.parityMasks) {
            Vldtn.requireTrue((mask & ~allowedBits) == 0,
                    "Parity mask contains bits outside bitCount");
        }
        weightStride = (setBitCount + 1) * syndromeCount;
        bitSyndromes = createBitSyndromes();
        counts = createCounts();
        domainSize = count(bitCount, setBitCount, paritySyndrome);
        Vldtn.requireTrue(domainSize > 0,
                "Fixed-weight parity domain must not be empty");
    }

    int bitCount() {
        return bitCount;
    }

    int setBitCount() {
        return setBitCount;
    }

    long[] parityMasks() {
        return parityMasks.clone();
    }

    int paritySyndrome() {
        return paritySyndrome;
    }

    long domainSize() {
        return domainSize;
    }

    /** Compares all parameters that define the order-preserving rank map. */
    boolean hasSameDomain(final LongFixedWeightRank other) {
        return bitCount == other.bitCount && setBitCount == other.setBitCount
                && paritySyndrome == other.paritySyndrome
                && Arrays.equals(parityMasks, other.parityMasks);
    }

    /** Maps a validated logical key to its zero-based numeric-order rank. */
    long rank(final long key) {
        validateKey(key);
        long rank = 0;
        int remaining = setBitCount;
        int syndrome = paritySyndrome;
        for (int bit = bitCount - 1; bit >= 0; bit--) {
            if ((key & (1L << bit)) != 0) {
                rank += count(bit, remaining, syndrome);
                remaining--;
                syndrome ^= bitSyndromes[bit];
            }
        }
        return rank;
    }

    /** Reconstructs a logical key, rejecting every rank outside the domain. */
    long unrank(final long rank) {
        validateRank(rank);
        long remainingRank = rank;
        long key = 0;
        int remaining = setBitCount;
        int syndrome = paritySyndrome;
        for (int bit = bitCount - 1; bit >= 0; bit--) {
            final long zeroCount = count(bit, remaining, syndrome);
            if (remainingRank >= zeroCount) {
                remainingRank -= zeroCount;
                key |= 1L << bit;
                remaining--;
                syndrome ^= bitSyndromes[bit];
            }
        }
        return key;
    }

    /** Validates an encoded rank without reconstructing its logical key. */
    void validateRank(final long rank) {
        if (rank < 0 || rank >= domainSize) {
            throw new IndexException(
                    "Fixed-weight rank is outside its domain.");
        }
    }

    private void validateKey(final long key) {
        if ((key & ~allowedBits) != 0 || Long.bitCount(key) != setBitCount) {
            throw new IndexException(
                    "Key does not belong to the fixed-weight bit domain.");
        }
        int syndrome = 0;
        for (int equation = 0; equation < parityMasks.length; equation++) {
            syndrome |= (Long.bitCount(key & parityMasks[equation])
                    & 1) << equation;
        }
        if (syndrome != paritySyndrome) {
            throw new IndexException(
                    "Key does not belong to the parity class.");
        }
    }

    private int[] createBitSyndromes() {
        final int[] result = new int[bitCount];
        for (int bit = 0; bit < bitCount; bit++) {
            for (int equation = 0; equation < parityMasks.length; equation++) {
                if ((parityMasks[equation] & (1L << bit)) != 0) {
                    result[bit] |= 1 << equation;
                }
            }
        }
        return result;
    }

    private long[] createCounts() {
        final long[] result = new long[(bitCount + 1) * weightStride];
        result[0] = 1;
        for (int bits = 1; bits <= bitCount; bits++) {
            for (int weight = 0; weight <= Math.min(bits,
                    setBitCount); weight++) {
                for (int syndrome = 0; syndrome < syndromeCount; syndrome++) {
                    final long zeros = result[(bits - 1) * weightStride
                            + weight * syndromeCount + syndrome];
                    final long ones = weight == 0 ? 0
                            : result[(bits - 1) * weightStride
                                    + (weight - 1) * syndromeCount
                                    + (syndrome ^ bitSyndromes[bits - 1])];
                    // C(63,31) fits a signed long; checked addition also guards
                    // this invariant if supported bounds change in the future.
                    result[bits * weightStride + weight * syndromeCount
                            + syndrome] = Math.addExact(zeros, ones);
                }
            }
        }
        return result;
    }

    private long count(final int bits, final int weight, final int syndrome) {
        return weight < 0 || weight > bits || weight > setBitCount ? 0
                : counts[bits * weightStride + weight * syndromeCount
                        + syndrome];
    }
}
