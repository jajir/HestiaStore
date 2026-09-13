package org.hestiastore.index.senku.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongToIntFunction;

import org.hestiastore.index.chunkentryfile.KeyPageCodec;
import org.hestiastore.index.chunkentryfile.KeyPageCodecs;
import org.hestiastore.index.chunkstore.Compression;
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
 * Compares complete primitive ranked flushes with uniform routing or a shard
 * containing 75 percent of a generation. Four million keys give the hot shard
 * three consecutive full-size pages. Immutable input maps are built once,
 * outside timing; each invocation has an independent in-memory destination.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2, jvmArgsAppend = { "-Xms1g", "-Xmx1g" })
public class SenkuSkewedFlushBenchmark {
    static final int SHARDS = 128;
    static final DataBlockSize BLOCK_SIZE = DataBlockSize.ofDataBlockSize(8192);

    @Param({ "4000000" })
    int entryCount = 4_000_000;

    @Param({ "uniform", "hot75" })
    String distribution = "hot75";

    private List<Map<Long, NullValue>> entries;
    private SenkuStorageFormat format;
    private LongToIntFunction router;
    private SenkuFlushWriter<Long, NullValue> writer;
    MemDirectory output;

    /** Builds valid 49-bit, 28-peg, four-parity keys and detached set maps. */
    @Setup(Level.Trial)
    public void setupTrial() {
        if (entryCount < 4 || !("uniform".equals(distribution)
                || "hot75".equals(distribution))) {
            throw new IllegalArgumentException("Invalid ranked flush fixture");
        }
        final KeyPageCodec<Long> codec = KeyPageCodecs
                .longFixedWeightDeltaVarint(49, 28,
                        new long[] { 3, 12, 48, 192 }, 0);
        format = new SenkuStorageFormat(codec, Compression.zstd(3));
        final long boundary = codec.decodeLongKey(997L * (entryCount * 3L / 4));
        router = key -> hash(key, boundary);
        entries = new ArrayList<>(SHARDS);
        for (int stripe = 0; stripe < SHARDS; stripe++) {
            entries.add(new SenkuLongSetMap(
                    Math.max(16, entryCount / SHARDS * 2), router));
        }
        for (int index = 0; index < entryCount; index++) {
            final long key = codec.decodeLongKey(index * 997L);
            final int hash = router.applyAsInt(key);
            ((SenkuLongSetMap) entries.get(SenkuIngestor.stripeFromHash(hash)))
                    .addLong(key, hash);
        }
    }

    /** Creates fresh output without modifying the shared input generation. */
    @Setup(Level.Invocation)
    public void setupInvocation() {
        output = new MemDirectory();
        writer = new SenkuFlushWriter<>(output, new TypeDescriptorLong(),
                new TypeDescriptorNull(), key -> router.applyAsInt(key), SHARDS,
                1_000_000, 10_000_000L, BLOCK_SIZE, format);
    }

    /**
     * Measures routing, sorting, unchanged rank/Zstd encoding and publication.
     */
    @Benchmark
    public void write() {
        writer.write(0L, entries);
    }

    private int hash(final long key, final long hotBoundary) {
        long mixed = key;
        mixed ^= mixed >>> 33;
        mixed *= 0xff51afd7ed558ccdL;
        mixed ^= mixed >>> 33;
        mixed *= 0xc4ceb9fe1a85ec53L;
        mixed ^= mixed >>> 33;
        final int hash = (int) (mixed ^ mixed >>> 32);
        if ("uniform".equals(distribution)) {
            return hash;
        }
        int shard = key < hotBoundary ? 5 : Math.floorMod(hash, SHARDS - 1);
        if (key >= hotBoundary && shard >= 5) {
            shard++;
        }
        return (hash & ~(SHARDS - 1)) | shard;
    }
}
