package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SenkuMergeEncodingBenchmarkTest {

    @Test
    void benchmarkExecutesGenericAndPrimitivePaths() {
        assertEquals(128L, run("generic"));
        assertEquals(128L, run("primitive"));
    }

    private static long run(final String path) {
        final SenkuMergeEncodingBenchmark benchmark =
                new SenkuMergeEncodingBenchmark();
        benchmark.entryCount = 128;
        benchmark.path = path;
        benchmark.dataBlockBytes = 8_192;
        benchmark.setupTrial();
        benchmark.setupInvocation();
        return benchmark.merge();
    }
}
