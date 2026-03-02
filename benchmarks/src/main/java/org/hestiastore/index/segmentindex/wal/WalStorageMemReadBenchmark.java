package org.hestiastore.index.segmentindex.wal;

import java.util.concurrent.TimeUnit;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
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
 * Measures in-memory WAL read performance and allocation profile.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class WalStorageMemReadBenchmark {

    private static final String FILE_NAME = "00000000000000000001.wal";

    @Param({ "64", "512", "4096" })
    private int readLength;

    @Param({ "0", "1024", "65536" })
    private int startPosition;

    @Param({ "1048576" })
    private int fileSizeBytes;

    private MemDirectory walDirectory;
    private WalStorageMem storage;
    private byte[] destination;

    @Setup(Level.Trial)
    public void setup() {
        walDirectory = new MemDirectory();
        storage = new WalStorageMem(walDirectory);
        final byte[] data = new byte[fileSizeBytes];
        for (int index = 0; index < data.length; index++) {
            data[index] = (byte) (index * 31 + 17);
        }
        walDirectory.setFileSequence(FILE_NAME, ByteSequences.wrap(data));
        validateParityOnce();
    }

    @Setup(Level.Iteration)
    public void setupDestination() {
        destination = new byte[readLength];
    }

    /**
     * Current implementation using {@link WalStorageMem#read(String, long, byte[], int, int)}.
     *
     * @return bytes read plus one byte-dependent checksum
     */
    @Benchmark
    public int readCurrent() {
        final int read = storage.read(FILE_NAME, startPosition, destination, 0,
                readLength);
        return fold(read);
    }

    /**
     * Legacy behavior that cloned the full file content before every read.
     *
     * @return bytes read plus one byte-dependent checksum
     */
    @Benchmark
    public int readLegacyFullCopyPath() {
        final int read = legacyRead(FILE_NAME, startPosition, destination, 0,
                readLength);
        return fold(read);
    }

    private int legacyRead(final String fileName, final long position,
            final byte[] target, final int targetOffset,
            final int targetLength) {
        final byte[] data = walDirectory.getFileSequence(fileName)
                .toByteArrayCopy();
        if (position >= data.length) {
            return -1;
        }
        final int available = (int) Math.min(targetLength,
                data.length - position);
        if (available <= 0) {
            return -1;
        }
        System.arraycopy(data, (int) position, target, targetOffset, available);
        return available;
    }

    private int fold(final int read) {
        if (read <= 0) {
            return read;
        }
        return read + destination[0];
    }

    /**
     * Cross-check that legacy and current implementations expose identical
     * read content for benchmark parameters.
     */
    private void validateParityOnce() {
        final byte[] left = new byte[readLength];
        final byte[] right = new byte[readLength];
        final int currentRead = storage.read(FILE_NAME, startPosition, left, 0,
                readLength);
        final int legacyRead = legacyRead(FILE_NAME, startPosition, right, 0,
                readLength);
        if (currentRead != legacyRead) {
            throw new IllegalStateException(String.format(
                    "Current read '%d' differs from legacy read '%d'",
                    currentRead, legacyRead));
        }
        if (currentRead > 0) {
            final ByteSequence leftSeq = ByteSequences.viewOf(left, 0,
                    currentRead);
            final ByteSequence rightSeq = ByteSequences.viewOf(right, 0,
                    currentRead);
            if (!ByteSequences.contentEquals(leftSeq, rightSeq)) {
                throw new IllegalStateException(
                        "Current read bytes differ from legacy read bytes");
            }
        }
    }
}
