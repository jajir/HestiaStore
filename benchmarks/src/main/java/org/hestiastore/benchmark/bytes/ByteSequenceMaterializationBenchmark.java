package org.hestiastore.benchmark.bytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.bytes.ConcatenatedByteSequence;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * First materialization of fresh balanced sequence trees. Backing arrays are
 * reused, but {@link #materialize()} creates fresh leaves and concatenations so
 * cached child arrays cannot hide the cost of reading a new page. A separate
 * control measures a fresh parent over previously materialized child trees.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 3, jvmArgsAppend = { "-Xms1g", "-Xmx1g" })
public class ByteSequenceMaterializationBenchmark {

    private static final int SLICE_OFFSET = 8;

    @Param({ "65536" })
    int length = 65536;

    @Param({ "1", "4", "16" })
    int leaves = 4;

    @Param({ "SLICE" })
    String leafType = "SLICE";

    private byte[][] backingArrays;
    private int leafOffset;
    private int leafLength;
    private ByteSequence cachedFirst;
    private ByteSequence cachedSecond;

    /** Creates deterministic backing arrays and verifies an independent tree. */
    @Setup
    public void setup() {
        if (length < 1 || leaves < 1 || length % leaves != 0
                || (!"SLICE".equals(leafType) && !"VIEW".equals(leafType))) {
            throw new IllegalArgumentException("Invalid materialization fixture");
        }
        leafLength = length / leaves;
        leafOffset = "SLICE".equals(leafType) ? SLICE_OFFSET : 0;
        backingArrays = new byte[leaves][];
        final byte[] expected = new byte[length];
        for (int leaf = 0; leaf < leaves; leaf++) {
            final byte[] backing = new byte[leafLength + 2 * leafOffset];
            for (int index = 0; index < leafLength; index++) {
                final int position = leaf * leafLength + index;
                final byte value = (byte) ((position * 31) ^ (position >>> 2));
                backing[leafOffset + index] = value;
                expected[position] = value;
            }
            backingArrays[leaf] = backing;
        }
        if (!Arrays.equals(expected, materialize())) {
            throw new IllegalStateException("Materialized bytes differ");
        }
        cachedFirst = createCachedHalf(expected, 0, length / 2);
        cachedSecond = createCachedHalf(expected, length / 2, length);
        if (!Arrays.equals(expected, cachedChildren())) {
            throw new IllegalStateException("Cached child bytes differ");
        }
    }

    /**
     * Creates fresh wrappers and assembles their bytes for the first time.
     *
     * @return newly materialized bytes, consumed by JMH
     */
    @Benchmark
    public byte[] materialize() {
        final List<ByteSequence> parts = new ArrayList<>(leaves);
        for (final byte[] backing : backingArrays) {
            parts.add(ByteSequences.viewOf(backing, leafOffset,
                    leafOffset + leafLength));
        }
        return ByteSequences.concatNonEmpty(parts).toByteArray();
    }

    /**
     * Materializes a fresh parent over two cached eight-leaf child trees.
     *
     * @return newly materialized bytes, consumed by JMH
     */
    @Benchmark
    public byte[] cachedChildren() {
        return ConcatenatedByteSequence.of(cachedFirst, cachedSecond)
                .toByteArray();
    }

    private ByteSequence createCachedHalf(final byte[] bytes, final int from,
            final int to) {
        final List<ByteSequence> parts = new ArrayList<>(8);
        for (int index = 0; index < 8; index++) {
            parts.add(ByteSequences.viewOf(bytes,
                    from + (to - from) * index / 8,
                    from + (to - from) * (index + 1) / 8));
        }
        final ByteSequence child = ByteSequences.concat(parts);
        child.toByteArray();
        return child;
    }
}
