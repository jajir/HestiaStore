package org.hestiastore.index.senku.internal;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.hestiastore.index.datatype.NullValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Measures uncontended batch-rotation bookkeeping as stripe count grows. The
 * benchmark covers locking every stripe, creating a lazy replacement map set,
 * and resetting the bounded count-sampling state.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3, jvmArgsAppend = { "-Xms2g", "-Xmx2g" })
public class SenkuIngestionRotationBenchmark {

    /**
     * Rotates one empty replacement batch while every mutation lock is held.
     *
     * @param state     persistent locks and selected stripe count
     * @param blackhole consumes replacement bookkeeping
     */
    @Benchmark
    public void rotateBatch(final RotationState state,
            final Blackhole blackhole) {
        state.lockAll();
        try {
            final List<Map<Long, NullValue>> maps =
                    SenkuIngestionMapBenchmark.newMaps(1_048_576,
                            state.stripeCount,
                            SenkuIngestionMapBenchmark.MIXED_OPEN_ADDRESSED);
            final int[] entryCounts = new int[state.stripeCount];
            final int[] publishedSizes = new int[state.stripeCount];
            final int[] nextChecks = new int[state.stripeCount];
            Arrays.fill(nextChecks,
                    Math.max(1, 1_048_576 / (state.stripeCount * 4)));
            blackhole.consume(maps);
            blackhole.consume(entryCounts);
            blackhole.consume(publishedSizes);
            blackhole.consume(nextChecks);
            blackhole.consume(new AtomicInteger());
        } finally {
            state.unlockAll();
        }
    }

    /**
     * Owns the persistent lock array for one benchmark thread.
     */
    @State(Scope.Thread)
    public static class RotationState {

        @Param({ "32", "64", "128" })
        int stripeCount = 32;

        private ReentrantLock[] locks;

        /** Creates the persistent lock set outside measured invocations. */
        @Setup(Level.Trial)
        public void setup() {
            locks = new ReentrantLock[stripeCount];
            for (int index = 0; index < locks.length; index++) {
                locks[index] = new ReentrantLock();
            }
        }

        private void lockAll() {
            for (final ReentrantLock lock : locks) {
                lock.lock();
            }
        }

        private void unlockAll() {
            for (int index = locks.length - 1; index >= 0; index--) {
                locks[index].unlock();
            }
        }
    }
}
