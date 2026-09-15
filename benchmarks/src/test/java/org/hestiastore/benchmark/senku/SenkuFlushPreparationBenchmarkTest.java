package org.hestiastore.benchmark.senku;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SenkuFlushPreparationBenchmarkTest {

    private SenkuFlushPreparationBenchmark benchmark;

    @BeforeEach
    void setUp() {
        benchmark = new SenkuFlushPreparationBenchmark();
        benchmark.entryCount = 4_096;
        benchmark.shardCount = 32;
        benchmark.setup();
    }

    @Test
    void preparationStrategiesProduceEquivalentBoundaries() {
        final long expected = benchmark.currentSingleArray();

        assertEquals(expected, benchmark.perShardArrays());
        assertEquals(expected, benchmark.prepartitionedShardMaps());
        assertEquals(expected, benchmark.concurrentShardSort());
    }

    @Test
    void ingestionLayoutsRetainEveryEntry() {
        assertEquals(benchmark.entryCount,
                entryCount(benchmark.buildMutationStripeMaps()));
        assertEquals(benchmark.entryCount,
                entryCount(benchmark.buildPersistentShardMaps()));
    }

    private int entryCount(final List<Map<Long, Long>> maps) {
        return maps.stream().mapToInt(Map::size).sum();
    }
}
