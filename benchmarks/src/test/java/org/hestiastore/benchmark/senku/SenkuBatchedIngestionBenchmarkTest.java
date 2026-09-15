package org.hestiastore.benchmark.senku;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class SenkuBatchedIngestionBenchmarkTest {
    @Test
    void bothSubmissionModesCompleteTheirWholeInput() {
        for (final String mode : new String[] { "single", "batch" }) {
            final SenkuParallelIngestionBenchmark.IngestionState state = new SenkuParallelIngestionBenchmark.IngestionState();
            state.api = "long-set";
            state.rotationThreshold = 1024;
            final SenkuBatchedIngestionBenchmark.BatchState batch = new SenkuBatchedIngestionBenchmark.BatchState();
            batch.submission = mode;
            final SenkuBatchedIngestionBenchmark benchmark = new SenkuBatchedIngestionBenchmark();
            final SenkuParallelIngestionBenchmark.IngestionThreadState sequence = new SenkuParallelIngestionBenchmark.IngestionThreadState();
            assertDoesNotThrow(() -> {
                state.setup();
                try {
                    benchmark.put(state, sequence, batch);
                } finally {
                    state.tearDown();
                }
            });
        }
    }

    @Test
    void genericStateCannotSilentlySubstitutePerKeyPuts() {
        final SenkuParallelIngestionBenchmark.IngestionState state = new SenkuParallelIngestionBenchmark.IngestionState();
        final long[] keys = { 1, 2 };
        assertThrows(IllegalStateException.class, () -> state.putLongs(keys));
    }
}
