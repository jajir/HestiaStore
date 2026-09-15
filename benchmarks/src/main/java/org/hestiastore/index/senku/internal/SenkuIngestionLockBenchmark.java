package org.hestiastore.index.senku.internal;

import java.util.concurrent.TimeUnit;

import org.hestiastore.index.datatype.NullValue;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures mutation-lock wait and hold time separately from the primary
 * ingestion throughput benchmark. Timing calls are intentionally confined to
 * this benchmark and add no instrumentation to the production hot path.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3, jvmArgsAppend = { "-Xms2g", "-Xmx2g" })
public class SenkuIngestionLockBenchmark {

    /**
     * Measures one duplicate update and exposes per-iteration lock wait and
     * hold nanosecond totals as secondary JMH results. Divide each total by
     * the operations completed in that iteration for nanoseconds per update.
     *
     * @param state    shared populated maps and locks
     * @param cursor   calling thread's deterministic key cursor
     * @param counters per-thread timing counters
     * @return existing non-null value
     */
    @Benchmark
    @Threads(8)
    public NullValue updateExistingEntry(
            final SenkuIngestionMapBenchmark.ExistingEntryState state,
            final SenkuIngestionMapBenchmark.KeyCursor cursor,
            final LockTimingCounters counters) {
        return state.updateExistingWithTiming(
                cursor.nextIndex(state.entryMask()), counters);
    }

    /**
     * Accumulates lock wait and hold nanoseconds as JMH event counters without
     * adding timing calls to production code.
     */
    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class LockTimingCounters {

        /** Nanoseconds spent waiting to acquire a mutation lock. */
        public long lockWaitNanoseconds;

        /** Nanoseconds spent in the mutation critical section. */
        public long lockHoldNanoseconds;

        /** Updates represented by the accumulated wait and hold totals. */
        public long operations;

        /**
         * Clears counters before each warmup and measurement iteration.
         */
        @Setup(Level.Iteration)
        public void reset() {
            lockWaitNanoseconds = 0L;
            lockHoldNanoseconds = 0L;
            operations = 0L;
        }

        /**
         * Adds one measured acquisition and critical section.
         *
         * @param waitNanoseconds acquisition duration
         * @param holdNanoseconds critical-section duration
         */
        void record(final long waitNanoseconds, final long holdNanoseconds) {
            lockWaitNanoseconds += waitNanoseconds;
            lockHoldNanoseconds += holdNanoseconds;
            operations++;
        }

        /**
         * Returns the updates observed since the iteration reset.
         *
         * @return measured update count
         */
        long operationCount() {
            return operations;
        }
    }
}
