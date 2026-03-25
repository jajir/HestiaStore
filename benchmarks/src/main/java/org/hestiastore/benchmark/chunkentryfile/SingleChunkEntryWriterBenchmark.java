package org.hestiastore.benchmark.chunkentryfile;

import java.util.concurrent.TimeUnit;

import org.hestiastore.index.Entry;
import org.hestiastore.index.chunkentryfile.SingleChunkEntryWriter;
import org.hestiastore.index.chunkentryfile.SingleChunkEntryWriterImpl;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorString;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures steady-state writes into {@link SingleChunkEntryWriterImpl}.
 * <p>
 * The benchmark reuses one writer until the configured chunk entry count is
 * reached, then closes the current payload and starts a new chunk. This keeps
 * memory bounded while still exercising the hot write path repeatedly.
 * </p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class SingleChunkEntryWriterBenchmark {

    private static final TypeDescriptorInteger KEY_DESCRIPTOR = new TypeDescriptorInteger();
    private static final TypeDescriptorString VALUE_DESCRIPTOR = new TypeDescriptorString();

    @Param({ "64", "256", "1024" })
    private int entriesPerChunk;

    @Param({ "16", "64" })
    private int valueLength;

    private Entry<Integer, String>[] entries;
    private SingleChunkEntryWriter<Integer, String> writer;
    private int entryIndex;

    @SuppressWarnings("unchecked")
    @Setup(Level.Iteration)
    public void setupWriter() {
        entries = (Entry<Integer, String>[]) new Entry<?, ?>[entriesPerChunk];
        for (int key = 0; key < entriesPerChunk; key++) {
            entries[key] = Entry.of(Integer.valueOf(key), buildValue(key));
        }
        writer = newWriter();
        entryIndex = 0;
    }

    @TearDown(Level.Iteration)
    public void tearDownWriter() {
        if (writer != null) {
            writer.closeSequence();
            writer = null;
        }
    }

    /**
     * Writes one ordered entry into the current chunk.
     *
     * @return checksum combining the current key and occasional payload length
     *         when a chunk is rotated
     */
    @Benchmark
    public int writeEntrySteadyState() {
        int checksum = 0;
        if (entryIndex == entries.length) {
            checksum = writer.closeSequence().length();
            writer = newWriter();
            entryIndex = 0;
        }
        final Entry<Integer, String> entry = entries[entryIndex];
        writer.put(entry);
        entryIndex++;
        return checksum ^ entry.getKey().intValue();
    }

    private SingleChunkEntryWriter<Integer, String> newWriter() {
        return new SingleChunkEntryWriterImpl<>(KEY_DESCRIPTOR,
                VALUE_DESCRIPTOR);
    }

    private String buildValue(final int key) {
        final String prefix = Integer.toString(key) + '-';
        final int suffixLength = Math.max(0, valueLength - prefix.length());
        return prefix + "x".repeat(suffixLength);
    }
}
