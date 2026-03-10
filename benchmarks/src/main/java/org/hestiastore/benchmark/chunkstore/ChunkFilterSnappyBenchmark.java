package org.hestiastore.benchmark.chunkstore;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.chunkstore.ChunkData;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterSnappyCompress;
import org.hestiastore.index.chunkstore.ChunkFilterSnappyDecompress;
import org.hestiastore.index.chunkstore.ChunkHeader;
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
import org.xerial.snappy.Snappy;

/**
 * Measures Snappy filter encode/decode paths with and without explicit input
 * cloning.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class ChunkFilterSnappyBenchmark {

    private static final int VERSION = 1;
    private static final long COMPRESSED_FLAG = 1L
            << ChunkFilter.BIT_POSITION_SNAPPY_COMPRESSION;

    @Param({ "128", "1008", "4096" })
    private int payloadSize;

    private ChunkFilterSnappyCompress compressFilter;
    private ChunkFilterSnappyDecompress decompressFilter;

    private byte[] contiguousPayload;
    private byte[] slicedPayloadBacking;
    private byte[] contiguousCompressed;
    private byte[] slicedCompressedBacking;

    @Setup
    public void setup() throws IOException {
        compressFilter = new ChunkFilterSnappyCompress();
        decompressFilter = new ChunkFilterSnappyDecompress();

        contiguousPayload = new byte[payloadSize];
        for (int index = 0; index < payloadSize; index++) {
            contiguousPayload[index] = (byte) (index * 13 + 7);
        }

        slicedPayloadBacking = new byte[payloadSize + 32];
        System.arraycopy(contiguousPayload, 0, slicedPayloadBacking, 11,
                payloadSize);

        contiguousCompressed = Snappy.compress(contiguousPayload);
        slicedCompressedBacking = new byte[contiguousCompressed.length + 32];
        System.arraycopy(contiguousCompressed, 0, slicedCompressedBacking, 9,
                contiguousCompressed.length);
    }

    @Benchmark
    public int compressCurrentContiguous() {
        final ByteSequence payload = ByteSequences.wrap(contiguousPayload);
        final ChunkData input = chunkData(payload, 0L);
        return compressFilter.apply(input).getPayloadSequence().length();
    }

    @Benchmark
    public int compressLegacyCopyContiguous() throws IOException {
        final ByteSequence payload = ByteSequences.wrap(contiguousPayload);
        return Snappy.compress(payload.toByteArrayCopy()).length;
    }

    @Benchmark
    public int compressCurrentSlice() {
        final ByteSequence payload = ByteSequences.viewOf(slicedPayloadBacking,
                11, 11 + payloadSize);
        final ChunkData input = chunkData(payload, 0L);
        return compressFilter.apply(input).getPayloadSequence().length();
    }

    @Benchmark
    public int compressLegacyCopySlice() throws IOException {
        final ByteSequence payload = ByteSequences.viewOf(slicedPayloadBacking,
                11, 11 + payloadSize);
        return Snappy.compress(payload.toByteArrayCopy()).length;
    }

    @Benchmark
    public int decompressCurrentContiguous() {
        final ByteSequence payload = ByteSequences.wrap(contiguousCompressed);
        final ChunkData input = chunkData(payload, COMPRESSED_FLAG);
        return decompressFilter.apply(input).getPayloadSequence().length();
    }

    @Benchmark
    public int decompressLegacyCopyContiguous() throws IOException {
        final ByteSequence payload = ByteSequences.wrap(contiguousCompressed);
        return Snappy.uncompress(payload.toByteArrayCopy()).length;
    }

    @Benchmark
    public int decompressCurrentSlice() {
        final ByteSequence payload = ByteSequences.viewOf(slicedCompressedBacking,
                9, 9 + contiguousCompressed.length);
        final ChunkData input = chunkData(payload, COMPRESSED_FLAG);
        return decompressFilter.apply(input).getPayloadSequence().length();
    }

    @Benchmark
    public int decompressLegacyCopySlice() throws IOException {
        final ByteSequence payload = ByteSequences.viewOf(slicedCompressedBacking,
                9, 9 + contiguousCompressed.length);
        return Snappy.uncompress(payload.toByteArrayCopy()).length;
    }

    private static ChunkData chunkData(final ByteSequence payload,
            final long flags) {
        return ChunkData.ofSequence(flags, 0L, ChunkHeader.MAGIC_NUMBER,
                VERSION, payload);
    }
}
