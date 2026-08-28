package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SenkuFlushOrderingBenchmarkTest {

    private SenkuFlushOrderingBenchmark benchmark;

    @BeforeEach
    void setUp() {
        benchmark = new SenkuFlushOrderingBenchmark();
        benchmark.entryCount = 4_096;
        benchmark.shardCount = 32;
        benchmark.setup();
    }

    @AfterEach
    void tearDown() {
        benchmark.tearDown();
    }

    @Test
    void compactStrategiesMatchEntryArrayBoundaries() {
        final long expected = benchmark.entryArrayBoundedTimSort();

        assertEquals(expected, benchmark.compactSerialSort());
        assertEquals(expected, benchmark.compactBoundedSort());
    }
}
