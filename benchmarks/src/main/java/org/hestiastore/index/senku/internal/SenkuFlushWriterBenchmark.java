package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.datatype.NullValue.NULL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hestiastore.benchmark.senku.SenkuParallelIngestionBenchmark;
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
 * Measures one complete in-memory Senku flush after detached mutation maps have
 * already been populated.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3, jvmArgsAppend = { "-Xms2g", "-Xmx2g" })
@State(Scope.Thread)
public class SenkuFlushWriterBenchmark {

    private static final int INGESTION_STRIPE_COUNT = 32;
    private static final int STRIPE_MIX_MULTIPLIER_1 = 0x7feb352d;
    private static final int STRIPE_MIX_MULTIPLIER_2 = 0x846ca68b;
    private static final DataBlockSize DATA_BLOCK_SIZE = DataBlockSize
            .ofDataBlockSize(8_192);

    @Param({ "1000000" })
    int entryCount = 1_000_000;

    @Param({ "32" })
    int shardCount = 32;

    private SenkuFlushWriter<Long, NullValue> writer;
    private List<Map<Long, NullValue>> entries;

    /**
     * Populates detached mutation maps outside the measured operation.
     */
    @Setup(Level.Invocation)
    public void setup() {
        final MemDirectory directory = new MemDirectory();
        writer = new SenkuFlushWriter<>(directory, new TypeDescriptorLong(),
                new TypeDescriptorNull(), key -> key.hashCode(), shardCount,
                entryCount, entryCount, DATA_BLOCK_SIZE);
        entries = newMaps();
        for (int sequence = 0; sequence < entryCount; sequence++) {
            final long key = SenkuParallelIngestionBenchmark
                    .biasedBoardKey(sequence);
            entries.get(stripe(key)).put(Long.valueOf(key), NULL);
        }
    }

    /**
     * Measures partitioning, sorting, page encoding, and the in-memory write.
     */
    @Benchmark
    public void write() {
        writer.write(0L, entries);
    }

    private List<Map<Long, NullValue>> newMaps() {
        final int totalCapacity = (int) ((4L * entryCount + 2L) / 3L);
        final int capacity = 1
                + (totalCapacity - 1) / INGESTION_STRIPE_COUNT;
        final List<Map<Long, NullValue>> maps = new ArrayList<>(
                INGESTION_STRIPE_COUNT);
        for (int index = 0; index < INGESTION_STRIPE_COUNT; index++) {
            maps.add(new HashMap<>(capacity));
        }
        return maps;
    }

    private int stripe(final long key) {
        int mixed = Long.hashCode(key);
        mixed ^= mixed >>> 16;
        mixed *= STRIPE_MIX_MULTIPLIER_1;
        mixed ^= mixed >>> 15;
        mixed *= STRIPE_MIX_MULTIPLIER_2;
        mixed ^= mixed >>> 16;
        return mixed & (INGESTION_STRIPE_COUNT - 1);
    }
}
