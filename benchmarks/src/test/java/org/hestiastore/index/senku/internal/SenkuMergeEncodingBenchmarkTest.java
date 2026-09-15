package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class SenkuMergeEncodingBenchmarkTest {
    @Test
    void fixedWeightProfileUsesTheSameLogicalKeysForDeltaAndRank() {
        for (final String storage : new String[] { "delta-zstd",
                "rank-zstd" }) {
            final var benchmark = new SenkuMergeEncodingBenchmark();
            benchmark.entryCount = 128;
            benchmark.keyShape = "fixed-weight";
            benchmark.storage = storage;
            benchmark.setupTrial();
            benchmark.setupInvocation();
            assertEquals(128, benchmark.merge());
        }
        final var invalid = new SenkuMergeEncodingBenchmark();
        invalid.storage = "rank-zstd";
        assertThrows(IllegalArgumentException.class, invalid::setupTrial);
        invalid.storage = "delta-zstd";
        invalid.keyShape = "unknown";
        assertThrows(IllegalArgumentException.class, invalid::setupTrial);
    }

    @Test
    void benchmarkExecutesGenericAndPrimitivePaths() {
        assertEquals(128L, run("generic"));
        assertEquals(128L, run("primitive"));
    }

    private static long run(final String path) {
        final SenkuMergeEncodingBenchmark benchmark = new SenkuMergeEncodingBenchmark();
        benchmark.entryCount = 128;
        benchmark.path = path;
        benchmark.dataBlockBytes = 8_192;
        benchmark.setupTrial();
        benchmark.setupInvocation();
        return benchmark.merge();
    }

    @Test
    void benchmarkExecutesDeltaCompressedAndUncompressedAndRejectsInvalidModes() {
        for (String storage : new String[] { "delta-zstd", "delta-none" }) {
            final var benchmark = new SenkuMergeEncodingBenchmark();
            benchmark.entryCount = 128;
            benchmark.storage = storage;
            benchmark.setupTrial();
            benchmark.setupInvocation();
            assertEquals(128L, benchmark.merge());
        }
        final var invalid = new SenkuMergeEncodingBenchmark();
        invalid.storage = "unknown";
        assertThrows(IllegalArgumentException.class, invalid::setupTrial);
        invalid.storage = "delta-zstd";
        invalid.path = "generic";
        assertThrows(IllegalArgumentException.class, invalid::setupTrial);
    }
}
