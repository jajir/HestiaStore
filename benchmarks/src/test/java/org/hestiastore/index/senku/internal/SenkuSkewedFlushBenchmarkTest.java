package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class SenkuSkewedFlushBenchmarkTest {
    @Test
    void fixturePublishesExactThreeQuarterHotShard() {
        final SenkuSkewedFlushBenchmark benchmark = new SenkuSkewedFlushBenchmark();
        benchmark.entryCount = 256;
        benchmark.setupTrial();
        benchmark.setupInvocation();
        benchmark.write();
        final SenkuShardIndex index = SenkuShardIndexCodec.read(
                benchmark.output.openSubDirectory("flush-00000"),
                SenkuSkewedFlushBenchmark.BLOCK_SIZE,
                SenkuSkewedFlushBenchmark.SHARDS);
        assertEquals(256, index.totalRecordCount());
        assertEquals(192, index.recordCount(5));
    }

    @Test
    void invalidDistributionFailsBeforeCreatingOutput() {
        final SenkuSkewedFlushBenchmark benchmark = new SenkuSkewedFlushBenchmark();
        benchmark.distribution = "unknown";
        assertThrows(IllegalArgumentException.class, benchmark::setupTrial);
    }
}
