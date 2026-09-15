package org.hestiastore.benchmark.chunkstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;

import org.hestiastore.index.bytes.ByteSequence;
import org.junit.jupiter.api.Test;

class ChunkStoreZstdReadBenchmarkTest {

    @Test
    void singleAndMultiblockReadsPreserveExactBytes() {
        for (final int length : new int[] { 4096, 65536 }) {
            final ChunkStoreZstdReadBenchmark benchmark = new ChunkStoreZstdReadBenchmark();
            benchmark.payloadSize = length;
            benchmark.setup();
            final byte[] expected = new byte[length];
            new Random(42).nextBytes(expected);
            System.arraycopy(expected, 0, expected, length / 2, length / 2);
            final ByteSequence first = benchmark.read();
            final ByteSequence second = benchmark.read();
            assertEquals(length, first.length());
            assertArrayEquals(expected, first.toByteArray());
            assertArrayEquals(expected, second.toByteArray());
            assertNotSame(first.toByteArray(), second.toByteArray());
            assertTrue(benchmark.compressedBytes < length);
            assertEquals(length == 65536, benchmark.compressedBytes > 8192);
        }
    }

    @Test
    void rejectsUnsupportedFixtureSize() {
        final ChunkStoreZstdReadBenchmark benchmark = new ChunkStoreZstdReadBenchmark();
        benchmark.payloadSize = 128;
        assertThrows(IllegalArgumentException.class, benchmark::setup);
    }
}
