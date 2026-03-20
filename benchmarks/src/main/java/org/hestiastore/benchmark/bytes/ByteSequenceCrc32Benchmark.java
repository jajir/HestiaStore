package org.hestiastore.benchmark.bytes;

import java.util.concurrent.TimeUnit;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequenceCrc32;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.bytes.MutableBytes;
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
 * Compares optimized {@link ByteSequenceCrc32#update(ByteSequence)} against
 * the previous per-byte virtual dispatch implementation.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class ByteSequenceCrc32Benchmark {

    @Param({ "VIEW", "SLICE", "MUTABLE" })
    private String sequenceType;

    @Param({ "128", "4096", "65536" })
    private int length;

    private ByteSequence sequence;
    private final ByteSequenceCrc32 optimized = new ByteSequenceCrc32();
    private final BaselineByteSequenceCrc32 baseline = new BaselineByteSequenceCrc32();

    @Setup
    public void setup() {
        switch (sequenceType) {
            case "VIEW":
                sequence = ByteSequences.wrap(buildPayload(length));
                break;
            case "SLICE":
                final byte[] slicedPayload = buildPayload(length + 32);
                sequence = ByteSequences.viewOf(slicedPayload, 16, 16 + length);
                break;
            case "MUTABLE":
                sequence = MutableBytes.wrap(buildPayload(length));
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported sequenceType: " + sequenceType);
        }
    }

    @Benchmark
    @SuppressWarnings("java:S100")
    public long baseline_previous_loop() {
        baseline.reset();
        baseline.update(sequence);
        return baseline.getValue();
    }

    @Benchmark
    @SuppressWarnings("java:S100")
    public long optimized_fast_path() {
        optimized.reset();
        optimized.update(sequence);
        return optimized.getValue();
    }

    private static byte[] buildPayload(final int payloadLength) {
        final byte[] payload = new byte[payloadLength];
        for (int index = 0; index < payload.length; index++) {
            payload[index] = (byte) ((index * 31) ^ (index >>> 2));
        }
        return payload;
    }

    private static final class BaselineByteSequenceCrc32 {

        private static final int[] CRC_TABLE = createTable();
        private int crc = 0xFFFFFFFF;

        void update(final ByteSequence sequence) {
            final ByteSequence validated = Vldtn.requireNonNull(sequence,
                    "sequence");
            int c = crc;
            final int sequenceLength = validated.length();
            for (int i = 0; i < sequenceLength; i++) {
                c = CRC_TABLE[(c ^ validated.getByte(i)) & 0xFF] ^ (c >>> 8);
            }
            crc = c;
        }

        long getValue() {
            return (crc ^ 0xFFFFFFFFL) & 0xFFFFFFFFL;
        }

        void reset() {
            crc = 0xFFFFFFFF;
        }

        private static int[] createTable() {
            final int[] table = new int[256];
            for (int n = 0; n < table.length; n++) {
                int c = n;
                for (int k = 8; --k >= 0;) {
                    if ((c & 1) != 0) {
                        c = 0xEDB88320 ^ (c >>> 1);
                    } else {
                        c = c >>> 1;
                    }
                }
                table[n] = c;
            }
            return table;
        }
    }
}
