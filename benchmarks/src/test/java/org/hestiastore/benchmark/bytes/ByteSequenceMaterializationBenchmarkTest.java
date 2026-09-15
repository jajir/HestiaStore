package org.hestiastore.benchmark.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ByteSequenceMaterializationBenchmarkTest {

    @Test
    void freshSliceTreesPreserveEveryByteAcrossInvocations() {
        final byte[] expected = new byte[65536];
        for (int index = 0; index < expected.length; index++) {
            expected[index] = (byte) ((index * 31) ^ (index >>> 2));
        }
        for (final int leaves : new int[] { 1, 4, 16 }) {
            final ByteSequenceMaterializationBenchmark benchmark = new ByteSequenceMaterializationBenchmark();
            benchmark.leaves = leaves;
            benchmark.setup();
            final byte[] first = benchmark.materialize();
            final byte[] second = benchmark.materialize();
            assertArrayEquals(expected, first);
            assertArrayEquals(expected, second);
            assertNotSame(first, second);
        }
    }

    @Test
    void flatViewControlCreatesFreshRootArrays() {
        final ByteSequenceMaterializationBenchmark benchmark = new ByteSequenceMaterializationBenchmark();
        benchmark.leafType = "VIEW";
        benchmark.leaves = 2;
        benchmark.length = 128;
        benchmark.setup();
        final byte[] first = benchmark.materialize();
        final byte[] second = benchmark.materialize();
        assertEquals(128, first.length);
        assertArrayEquals(first, second);
        assertNotSame(first, second);
    }

    @Test
    void cachedChildControlPreservesBytesWithFreshParents() {
        final ByteSequenceMaterializationBenchmark benchmark = new ByteSequenceMaterializationBenchmark();
        benchmark.leaves = 16;
        benchmark.setup();
        final byte[] first = benchmark.cachedChildren();
        final byte[] second = benchmark.cachedChildren();
        assertArrayEquals(benchmark.materialize(), first);
        assertArrayEquals(first, second);
        assertNotSame(first, second);
    }

    @Test
    void rejectsInvalidFixture() {
        final ByteSequenceMaterializationBenchmark benchmark = new ByteSequenceMaterializationBenchmark();
        benchmark.leaves = 3;
        assertThrows(IllegalArgumentException.class, benchmark::setup);
        benchmark.leaves = 4;
        benchmark.leafType = "UNKNOWN";
        assertThrows(IllegalArgumentException.class, benchmark::setup);
    }
}
