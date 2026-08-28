package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.datatype.NullValue.NULL;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.MemDirectory;
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

/**
 * Measures complete maintenance merge decoding, duplicate reduction, page
 * encoding, compression, and in-memory chunk-store writes. The generic variant
 * uses a semantically identical descriptor subclass to disable the exact-type
 * primitive-key specialization. Use {@code -prof gc} for allocation and GC
 * metrics and JFR for peak heap and phase attribution.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 4, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 2, jvmArgsAppend = { "-Xms1g", "-Xmx1g" })
@State(Scope.Thread)
public class SenkuMergeEncodingBenchmark {

    private static final int SOURCE_COUNT = 4;

    @Param({ "100000" })
    int entryCount = 100_000;

    @Param({ "generic", "primitive" })
    String path = "primitive";

    @Param({ "8192", "32768", "131072" })
    int dataBlockBytes = 8_192;

    private final TypeDescriptorNull values = new TypeDescriptorNull();
    private TypeDescriptorLong keys;
    private DataBlockSize dataBlockSize;
    private int pageEntryLimit;
    private List<SenkuMergeSource> sources;
    private SenkuMergeJob<Long, NullValue> job;

    /**
     * Creates reusable immutable sorted sources outside measured invocations.
     */
    @Setup(Level.Trial)
    public void setupTrial() {
        keys = "generic".equals(path) ? new TypeDescriptorLong() {
            // Exact-type selection intentionally disabled for the baseline.
        } : new TypeDescriptorLong();
        dataBlockSize = DataBlockSize.ofDataBlockSize(dataBlockBytes);
        pageEntryLimit = Math.min(65_536, entryCount);
        sources = new ArrayList<>(SOURCE_COUNT);
        for (int source = 0; source < SOURCE_COUNT; source++) {
            sources.add(createSource(source));
        }
        sources = List.copyOf(sources);
    }

    /**
     * Creates a fresh destination because each merge publishes immutable
     * output files and a manifest.
     */
    @Setup(Level.Invocation)
    public void setupInvocation() {
        job = new SenkuMergeJob<>(sources, new MemDirectory(), 0, 1, 0L,
                keys, values, (key, first, second) -> NULL, pageEntryLimit,
                entryCount, dataBlockSize);
    }

    /**
     * Executes one full maintenance merge.
     *
     * @return output record count consumed by JMH
     */
    @Benchmark
    public long merge() {
        return job.execute().manifest().recordCount();
    }

    private SenkuMergeSource createSource(final int source) {
        final int sourceEntries = entryCount / SOURCE_COUNT;
        final List<Entry<Long, NullValue>> entries = new ArrayList<>(
                sourceEntries);
        for (int index = 0; index < sourceEntries; index++) {
            final long key = (long) index * SOURCE_COUNT + source;
            entries.add(Entry.of(key, NULL));
        }
        final MemDirectory directory = new MemDirectory();
        final SenkuRunManifest manifest = new SenkuRunWriter<>(directory,
                new TypeDescriptorLong(), values, pageEntryLimit, entryCount,
                dataBlockSize).write(new EntryIteratorList<>(entries));
        return SenkuMergeSource.run(new LargeFile(directory, dataBlockSize,
                entryCount, manifest.partCount()), manifest.recordCount());
    }
}
