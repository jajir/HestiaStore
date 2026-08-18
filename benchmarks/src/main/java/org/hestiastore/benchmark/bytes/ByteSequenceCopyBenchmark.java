package org.hestiastore.benchmark.bytes;

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
import org.openjdk.jmh.infra.Blackhole;

/**
 * Measures copying from contiguous and concatenated byte-sequence layouts.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class ByteSequenceCopyBenchmark {

    private static final int TARGET_OFFSET = 8;

    @Param({ "VIEW", "CONCAT2", "NESTED4" })
    private String layout;

    @Param({ "128", "4096", "65536" })
    private int length;

    @Param({ "FIRST", "SECOND", "CROSSING", "FULL" })
    private String range;

    private ByteSequence source;
    private byte[] sourceBytes;
    private byte[] target;
    private int sourceOffset;
    private int copyLength;

    /**
     * Creates the selected source layout and validates the copied range once.
     */
    @Setup
    public void setup() {
        sourceBytes = buildPayload(length);
        source = createSource();
        configureRange();
        target = new byte[copyLength + (TARGET_OFFSET * 2)];
        source.copyTo(sourceOffset, target, TARGET_OFFSET, copyLength);
        validateCopiedRange();
    }

    /**
     * Copies the configured source range into a reusable target array.
     *
     * @param blackhole prevents elimination of the destination writes
     */
    @Benchmark
    public void copy(final Blackhole blackhole) {
        source.copyTo(sourceOffset, target, TARGET_OFFSET, copyLength);
        blackhole.consume(target);
    }

    private ByteSequence createSource() {
        if ("VIEW".equals(layout)) {
            return ByteSequences.wrap(sourceBytes);
        }
        if ("CONCAT2".equals(layout)) {
            final int middle = length / 2;
            return ConcatenatedByteSequence.of(
                    ByteSequences.viewOf(sourceBytes, 0, middle),
                    ByteSequences.viewOf(sourceBytes, middle, length));
        }
        if ("NESTED4".equals(layout)) {
            final int quarter = length / 4;
            return ByteSequences.concatNonEmpty(List.of(
                    ByteSequences.viewOf(sourceBytes, 0, quarter),
                    ByteSequences.viewOf(sourceBytes, quarter, quarter * 2),
                    ByteSequences.viewOf(sourceBytes, quarter * 2,
                            quarter * 3),
                    ByteSequences.viewOf(sourceBytes, quarter * 3, length)));
        }
        throw new IllegalArgumentException("Unsupported layout: " + layout);
    }

    private void configureRange() {
        final int quarter = length / 4;
        if ("FIRST".equals(range)) {
            sourceOffset = 0;
            copyLength = quarter;
            return;
        }
        if ("SECOND".equals(range)) {
            sourceOffset = quarter * 3;
            copyLength = quarter;
            return;
        }
        if ("CROSSING".equals(range)) {
            sourceOffset = quarter;
            copyLength = quarter * 2;
            return;
        }
        if ("FULL".equals(range)) {
            sourceOffset = 0;
            copyLength = length;
            return;
        }
        throw new IllegalArgumentException("Unsupported range: " + range);
    }

    private void validateCopiedRange() {
        final byte[] expected = Arrays.copyOfRange(sourceBytes, sourceOffset,
                sourceOffset + copyLength);
        final byte[] actual = Arrays.copyOfRange(target, TARGET_OFFSET,
                TARGET_OFFSET + copyLength);
        if (!Arrays.equals(expected, actual)) {
            throw new IllegalStateException("Copied range does not match source");
        }
    }

    private static byte[] buildPayload(final int payloadLength) {
        final byte[] payload = new byte[payloadLength];
        for (int index = 0; index < payload.length; index++) {
            payload[index] = (byte) ((index * 31) ^ (index >>> 2));
        }
        return payload;
    }
}
