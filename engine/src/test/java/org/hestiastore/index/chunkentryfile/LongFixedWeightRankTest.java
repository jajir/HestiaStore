package org.hestiastore.index.chunkentryfile;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;
import java.util.stream.LongStream;

import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class LongFixedWeightRankTest {

    @Test
    void rankValidationAndDomainEqualityDoNotRequireKeyReconstruction() {
        final LongFixedWeightRank domain = new LongFixedWeightRank(4, 2,
                new long[] { 3, 5 }, 1);
        assertDoesNotThrow(() -> domain.validateRank(0));
        final long lastRank = domain.domainSize() - 1;
        assertDoesNotThrow(() -> domain.validateRank(lastRank));
        assertThrows(IndexException.class, () -> domain.validateRank(-1));
        final long outOfRange = domain.domainSize();
        assertThrows(IndexException.class,
                () -> domain.validateRank(outOfRange));
        assertTrue(domain.hasSameDomain(
                new LongFixedWeightRank(4, 2, new long[] { 3, 5 }, 1)));
        assertFalse(domain.hasSameDomain(
                new LongFixedWeightRank(5, 2, new long[] { 3, 5 }, 1)));
        assertFalse(domain.hasSameDomain(
                new LongFixedWeightRank(4, 1, new long[] { 3, 5 }, 1)));
        assertFalse(domain.hasSameDomain(
                new LongFixedWeightRank(4, 2, new long[] { 3, 5 }, 2)));
        assertFalse(domain.hasSameDomain(
                new LongFixedWeightRank(4, 2, new long[] { 5, 3 }, 1)));
    }

    @Test
    void exhaustiveSmallDomainsAreDenseMonotoneAndExactlyReversible() {
        for (int bits = 1; bits <= 9; bits++) {
            final long mask = (1L << bits) - 1;
            for (int weight = 0; weight <= bits; weight++) {
                verifyDomain(bits, weight, new long[0], 0);
                for (int syndrome = 0; syndrome < 4; syndrome++) {
                    verifyDomain(bits, weight,
                            new long[] { 0x155L & mask, 0x12fL & mask },
                            syndrome);
                }
            }
        }
    }

    @Test
    void maximumWidthAndRankDoNotOverflowAndSparseExtremesRoundTrip() {
        final LongFixedWeightRank domain = new LongFixedWeightRank(63, 31,
                new long[0], 0);
        assertEquals(916312070471295267L, domain.domainSize());
        assertEquals((1L << 31) - 1, domain.unrank(0));
        assertEquals(((1L << 31) - 1) << 32,
                domain.unrank(domain.domainSize() - 1));
        final Random random = new Random(712);
        for (int i = 0; i < 2_000; i++) {
            final long rank = (random.nextLong() & Long.MAX_VALUE)
                    % domain.domainSize();
            assertEquals(rank, domain.rank(domain.unrank(rank)));
        }
        assertEquals(0,
                new LongFixedWeightRank(63, 0, new long[0], 0).unrank(0));
        assertEquals(Long.MAX_VALUE,
                new LongFixedWeightRank(63, 63, new long[0], 0).unrank(0));
    }

    @Test
    void masksAndReturnedParametersAreDefensivelyCopied() {
        final long[] masks = { 3, 5 };
        final LongFixedWeightRank domain = new LongFixedWeightRank(4, 2, masks,
                1);
        final long first = domain.unrank(0);
        masks[0] = 0;
        domain.parityMasks()[1] = 0;
        assertArrayEquals(new long[] { 3, 5 }, domain.parityMasks());
        assertEquals(4, domain.bitCount());
        assertEquals(2, domain.setBitCount());
        assertEquals(1, domain.paritySyndrome());
        assertEquals(first, domain.unrank(0));
        assertEquals(0, domain.rank(first));
    }

    @Test
    void rejectsInvalidConfigurationAndEmptyParityClasses() {
        assertThrows(IllegalArgumentException.class,
                () -> new LongFixedWeightRank(0, 0, new long[0], 0));
        assertThrows(IllegalArgumentException.class,
                () -> new LongFixedWeightRank(64, 0, new long[0], 0));
        assertThrows(IllegalArgumentException.class,
                () -> new LongFixedWeightRank(4, -1, new long[0], 0));
        assertThrows(IllegalArgumentException.class,
                () -> new LongFixedWeightRank(4, 5, new long[0], 0));
        assertThrows(IllegalArgumentException.class,
                () -> new LongFixedWeightRank(4, 2, null, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new LongFixedWeightRank(4, 2, new long[9], 0));
        assertThrows(IllegalArgumentException.class,
                () -> new LongFixedWeightRank(4, 2, new long[] { 16 }, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new LongFixedWeightRank(4, 2, new long[] { -1 }, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new LongFixedWeightRank(4, 2, new long[0], 1));
        assertThrows(IllegalArgumentException.class,
                () -> new LongFixedWeightRank(4, 2, new long[] { 0 }, -1));
        assertThrows(IllegalArgumentException.class,
                () -> new LongFixedWeightRank(4, 2, new long[] { 0 }, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new LongFixedWeightRank(4, 2, new long[] { 3, 3 }, 1));
        assertEquals(6,
                new LongFixedWeightRank(4, 2, new long[8], 0).domainSize());
    }

    @Test
    void rejectsWrongLogicalDomainAndOutOfRangeRanks() {
        final LongFixedWeightRank domain = new LongFixedWeightRank(4, 2,
                new long[] { 3 }, 0);
        assertThrows(IndexException.class, () -> domain.rank(17));
        assertThrows(IndexException.class, () -> domain.rank(-1));
        assertThrows(IndexException.class, () -> domain.rank(1));
        assertThrows(IndexException.class, () -> domain.rank(5));
        assertThrows(IndexException.class, () -> domain.unrank(-1));
        final long size = domain.domainSize();
        assertThrows(IndexException.class, () -> domain.unrank(size));
        assertThrows(IndexException.class, () -> domain.unrank(Long.MAX_VALUE));
    }

    private static void verifyDomain(final int bits, final int weight,
            final long[] masks, final int syndrome) {
        final long[] expected = LongStream.range(0, 1L << bits)
                .filter(key -> Long.bitCount(key) == weight
                        && syndrome(key, masks) == syndrome)
                .toArray();
        if (expected.length == 0) {
            assertThrows(IllegalArgumentException.class,
                    () -> new LongFixedWeightRank(bits, weight, masks,
                            syndrome));
            return;
        }
        final LongFixedWeightRank domain = new LongFixedWeightRank(bits, weight,
                masks, syndrome);
        assertEquals(expected.length, domain.domainSize());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(i, domain.rank(expected[i]));
            assertEquals(expected[i], domain.unrank(i));
        }
    }

    private static int syndrome(final long key, final long[] masks) {
        int value = 0;
        for (int i = 0; i < masks.length; i++) {
            value |= (Long.bitCount(key & masks[i]) & 1) << i;
        }
        return value;
    }
}
