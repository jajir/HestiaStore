package org.hestiastore.benchmark.bytes;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequenceCrc32;
import org.hestiastore.index.bytes.ByteSequences;
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
 * Compares CRC32 implementations on ByteSequence payloads.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class Crc32Benchmark {

    @Param({ "16", "64", "256", "1024", "4096", "16384" })
    private int payloadSize;

    @Param({ "VIEW", "SLICE", "CONCAT4" })
    private String layout;

    private ByteSequence sequence;
    private byte[] contiguous;

    @Setup(Level.Trial)
    public void setupSequence() {
        byte[] source = new byte[payloadSize + 8];
        for (int i = 0; i < source.length; i++) {
            source[i] = (byte) (i * 31 + 7);
        }

        if ("VIEW".equals(layout)) {
            contiguous = Arrays.copyOf(source, payloadSize);
            sequence = ByteSequences.wrap(contiguous);
            return;
        }
        if ("SLICE".equals(layout)) {
            contiguous = Arrays.copyOfRange(source, 3, 3 + payloadSize);
            sequence = ByteSequences.viewOf(source, 3, 3 + payloadSize);
            return;
        }
        if ("CONCAT4".equals(layout)) {
            contiguous = Arrays.copyOf(source, payloadSize);
            sequence = createConcatenated(contiguous);
            return;
        }
        throw new IllegalArgumentException(
                "Unsupported layout '" + layout + "'");
    }

    @Benchmark
    public long byteSequenceCrc32Current() {
        final ByteSequenceCrc32 crc = new ByteSequenceCrc32();
        crc.update(sequence);
        return crc.getValue();
    }

    @Benchmark
    public long jdkCrc32OnByteArray() {
        final CRC32 crc = new CRC32();
        crc.update(contiguous, 0, contiguous.length);
        return crc.getValue();
    }

    @Benchmark
    public long pureJavaReferenceOnByteSequence() {
        final PureJavaReferenceCrc32 crc = new PureJavaReferenceCrc32();
        crc.update(sequence);
        return crc.getValue();
    }

    private static ByteSequence createConcatenated(final byte[] data) {
        Vldtn.requireNonNull(data, "data");
        final int length = data.length;
        final int quarter = Math.max(1, length / 4);
        final int s1 = quarter;
        final int s2 = Math.min(length, s1 + quarter);
        final int s3 = Math.min(length, s2 + quarter);
        final ByteSequence p1 = ByteSequences.viewOf(data, 0, s1);
        final ByteSequence p2 = ByteSequences.viewOf(data, s1, s2);
        final ByteSequence p3 = ByteSequences.viewOf(data, s2, s3);
        final ByteSequence p4 = ByteSequences.viewOf(data, s3, length);
        return ByteSequences.concatNonEmpty(List.of(p1, p2, p3, p4));
    }

    /**
     * Stable baseline implementation equivalent to previous pure-Java loop.
     */
    private static final class PureJavaReferenceCrc32 {

        private static final int[] CRC_TABLE = new int[256];

        static {
            for (int n = 0; n < 256; n++) {
                int c = n;
                for (int k = 8; --k >= 0;) {
                    if ((c & 1) != 0) {
                        c = 0xEDB88320 ^ (c >>> 1);
                    } else {
                        c = c >>> 1;
                    }
                }
                CRC_TABLE[n] = c;
            }
        }

        private int crc = 0xFFFFFFFF;

        void update(final ByteSequence sequence) {
            final ByteSequence validated = Vldtn.requireNonNull(sequence,
                    "sequence");
            int c = crc;
            final int length = validated.length();
            for (int i = 0; i < length; i++) {
                c = CRC_TABLE[(c ^ validated.getByte(i)) & 0xFF] ^ (c >>> 8);
            }
            crc = c;
        }

        long getValue() {
            return (crc ^ 0xFFFFFFFFL) & 0xFFFFFFFFL;
        }
    }
}
