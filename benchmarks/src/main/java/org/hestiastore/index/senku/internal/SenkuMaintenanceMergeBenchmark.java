package org.hestiastore.index.senku.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.chunkentryfile.KeyPageCodec;
import org.hestiastore.index.chunkentryfile.KeyPageCodecs;
import org.hestiastore.index.chunkstore.Compression;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.senku.SenkuMergeFunctions;
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
 * Complete ranked set merges with interleaved sources, including the live
 * 64-way maintenance shape. Inputs are immutable and generated outside timing;
 * output uses unchanged one-million-key pages, 8 KiB blocks, and Zstd 3.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 4, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 2, jvmArgsAppend = { "-Xms1g", "-Xmx1g" })
public class SenkuMaintenanceMergeBenchmark {
    static final DataBlockSize BLOCK_SIZE = DataBlockSize.ofDataBlockSize(8192);
    static final int PAGE_KEYS = 1_000_000;

    @Param({ "1000000" })
    int entryCount = 1_000_000;

    @Param({ "4", "64" })
    int sourceCount = 64;

    @Param({ "0", "50" })
    int duplicatePercent;

    private List<SenkuMergeSource> sources;
    private SenkuMergeJob<Long, NullValue> job;
    private SenkuStorageFormat format;
    private final TypeDescriptorLong keys = new TypeDescriptorLong();
    private final TypeDescriptorNull values = new TypeDescriptorNull();
    MemDirectory output;

    /** Builds sorted fixed-population sources with deterministic rank gaps. */
    @Setup(Level.Trial)
    public void setupTrial() {
        if (sourceCount < 1 || entryCount < sourceCount
                || entryCount % sourceCount != 0
                || (duplicatePercent != 0 && duplicatePercent != 50)
                || (duplicatePercent == 50 && sourceCount % 2 != 0)) {
            throw new IllegalArgumentException("Invalid maintenance fixture");
        }
        final KeyPageCodec<Long> codec = KeyPageCodecs
                .longFixedWeightDeltaVarint(49, 30,
                        new long[] { 3, 12, 48, 192 }, 0);
        format = new SenkuStorageFormat(codec, Compression.zstd(3));
        final List<SenkuMergeSource> inputs = new ArrayList<>(sourceCount);
        final int uniqueSources = duplicatePercent == 0 ? sourceCount
                : sourceCount / 2;
        for (int source = 0; source < sourceCount; source++) {
            final int sourceOrdinal = duplicatePercent == 0 ? source
                    : source / 2;
            final List<Entry<Long, NullValue>> entries = new ArrayList<>(
                    entryCount / sourceCount);
            for (int index = 0; index < entryCount / sourceCount; index++) {
                final long ordinal = (long) index * uniqueSources
                        + sourceOrdinal;
                entries.add(Entry.of(codec.decodeLongKey(encodedRank(ordinal)),
                        NullValue.NULL));
            }
            final MemDirectory input = new MemDirectory();
            final SenkuRunManifest manifest = new SenkuRunWriter<>(input, keys,
                    values, PAGE_KEYS, 10_000_000L, BLOCK_SIZE, () -> true,
                    format).write(new EntryIteratorList<>(entries));
            inputs.add(
                    SenkuMergeSource.run(
                            new LargeFile(input, BLOCK_SIZE, 10_000_000L,
                                    manifest.partCount()),
                            manifest.recordCount()));
        }
        sources = List.copyOf(inputs);
    }

    /** Creates fresh immutable-publication output for each measured merge. */
    @Setup(Level.Invocation)
    public void setupInvocation() {
        output = new MemDirectory();
        job = new SenkuMergeJob<>(sources, output, 0, 1, 0L, keys, values,
                SenkuMergeFunctions.longSet(), PAGE_KEYS, 10_000_000L,
                BLOCK_SIZE, () -> true, format);
    }

    /** Runs the complete merge and rejects incomplete output counts. */
    @Benchmark
    public long merge() {
        final long count = job.execute().manifest().recordCount();
        final long expected = duplicatePercent == 0 ? entryCount
                : entryCount / 2;
        if (count != expected) {
            throw new IllegalStateException("Incomplete benchmark merge");
        }
        return count;
    }

    /** Returns the immutable codec for independent output verification. */
    KeyPageCodec<Long> codec() {
        return format.keyCodec();
    }

    /** Strictly increasing ranks with reproducible nonconstant gaps. */
    static long encodedRank(final long ordinal) {
        long mixed = ordinal * 0x9e3779b97f4a7c15L;
        mixed ^= mixed >>> 32;
        return ordinal * 1024 + (mixed & 1023);
    }
}
