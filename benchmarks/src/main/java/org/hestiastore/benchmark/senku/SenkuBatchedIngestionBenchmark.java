package org.hestiastore.benchmark.senku;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Compares the same 4096 generated keys submitted singly or through the real
 * synchronous primitive batch API. Scores count keys, not batch calls. Use
 * api=long-set for like-for-like comparisons and record maintenance-phase
 * noise.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2, jvmArgsAppend = { "-Xms1g", "-Xmx1g" })
@Threads(8)
public class SenkuBatchedIngestionBenchmark {
    static final int KEYS_PER_CALL = 4096;

    /**
     * Fills a reusable caller array identically for both API modes, then admits
     * every key before returning; no keys remain buffered across invocations.
     */
    @Benchmark
    @OperationsPerInvocation(KEYS_PER_CALL)
    public void put(final SenkuParallelIngestionBenchmark.IngestionState state,
            final SenkuParallelIngestionBenchmark.IngestionThreadState sequence,
            final BatchState batch) {
        for (int index = 0; index < batch.keys.length; index++) {
            batch.keys[index] = sequence.nextKey();
        }
        if ("batch".equals(batch.submission)) {
            state.putLongs(batch.keys);
        } else if ("single".equals(batch.submission)) {
            for (final long key : batch.keys) {
                state.put(key);
            }
        } else {
            throw new IllegalArgumentException("Unknown submission mode");
        }
    }

    /**
     * Thread-confined input storage reused only after synchronous admission.
     */
    @State(Scope.Thread)
    public static class BatchState {
        @Param({ "single", "batch" })
        String submission = "batch";
        final long[] keys = new long[KEYS_PER_CALL];
    }
}
