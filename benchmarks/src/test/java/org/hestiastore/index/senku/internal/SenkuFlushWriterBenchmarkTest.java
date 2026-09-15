package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Test;

class SenkuFlushWriterBenchmarkTest {

    @Test
    void benchmarkWritesPreparedBatch() {
        final SenkuFlushWriterBenchmark benchmark =
                new SenkuFlushWriterBenchmark();
        benchmark.entryCount = 128;
        benchmark.shardCount = 8;
        benchmark.setup();

        assertDoesNotThrow(benchmark::write);
    }
}
