package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class SenkuMetadataDiscoveryBenchmarkTest {
    @Test
    void repeatedDiscoveryRetainsEmptyAndSummarizedRunsWithoutMerging() {
        for (final int samples : new int[] { 0, 16 }) {
            final SenkuMetadataDiscoveryBenchmark benchmark = new SenkuMetadataDiscoveryBenchmark();
            benchmark.shardCount = 2;
            benchmark.runsPerShard = 4;
            benchmark.summarySize = samples;
            try {
                benchmark.setupTrial();
                assertEquals(8, benchmark.scan());
                assertEquals(8, benchmark.scan());
            } finally {
                benchmark.tearDown();
            }
        }
    }

    @Test
    void rejectsInvalidFixtureWithoutStartingWorkers() {
        final SenkuMetadataDiscoveryBenchmark benchmark = new SenkuMetadataDiscoveryBenchmark();
        benchmark.summarySize = -1;
        assertThrows(IllegalArgumentException.class, benchmark::setupTrial);
        benchmark.summarySize = 0;
        benchmark.runsPerShard = 0;
        assertThrows(IllegalArgumentException.class, benchmark::setupTrial);
        benchmark.tearDown();
    }
}
