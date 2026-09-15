package org.hestiastore.benchmark.chunkstore;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.bytes.ConcatenatedByteSequence;
import org.hestiastore.index.chunkstore.Chunk;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterZstdCompress;
import org.hestiastore.index.chunkstore.ChunkFilterZstdDecompress;
import org.hestiastore.index.chunkstore.ChunkStoreFile;
import org.hestiastore.index.chunkstore.ChunkStoreReader;
import org.hestiastore.index.chunkstore.ChunkStoreWriter;
import org.hestiastore.index.chunkstore.ChunkStoreWriterTx;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.MemDirectory;
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
 * Complete chunk reads from immutable in-memory storage, including block reads,
 * CRC validation, compressed input materialization, and Zstd decompression.
 * Setup verifies compression, the expected block layout, and every decoded byte.
 * No page, reader, or materialized sequence is reused between measured reads.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 3, jvmArgsAppend = { "-Xms1g", "-Xmx1g" })
public class ChunkStoreZstdReadBenchmark {

    private static final String FILE_NAME = "zstd-read-benchmark";
    private static final DataBlockSize BLOCK_SIZE = DataBlockSize
            .ofDataBlockSize(8192);
    private static final List<ChunkFilter> ENCODING = List.of(
            new ChunkFilterZstdCompress(3), new ChunkFilterCrc32Writing());

    @Param({ "4096", "65536" })
    int payloadSize = 65536;

    private ChunkStoreFile store;
    int compressedBytes;
    byte[] persistedBytes;

    /** Writes a deterministic half-repeated payload and verifies the fixture. */
    @Setup
    public void setup() {
        if (payloadSize != 4096 && payloadSize != 65536) {
            throw new IllegalArgumentException("Unsupported Zstd fixture size");
        }
        final byte[] payload = new byte[payloadSize];
        new Random(42).nextBytes(payload);
        System.arraycopy(payload, 0, payload, payloadSize / 2, payloadSize / 2);
        final MemDirectory directory = new MemDirectory();
        store = new ChunkStoreFile(directory, FILE_NAME, BLOCK_SIZE, ENCODING,
                List.of(new ChunkFilterCrc32Validation(),
                        new ChunkFilterZstdDecompress()));
        final ChunkStoreWriterTx transaction = store.openWriteTx();
        try (ChunkStoreWriter writer = transaction.open()) {
            writer.writeSequence(ByteSequences.wrap(payload), 1);
        }
        transaction.commit();
        persistedBytes = directory.getFileSequence(FILE_NAME).toByteArray();
        verifyStoredLayout(directory);
        if (!Arrays.equals(payload, read().toByteArray())) {
            throw new IllegalStateException("Decoded Zstd payload differs");
        }
    }

    /**
     * Opens, decodes, and closes one complete chunk reader.
     *
     * @return decoded payload, consumed by JMH
     */
    @Benchmark
    public ByteSequence read() {
        try (ChunkStoreReader reader = store
                .openReader(store.getFirstChunkStorePosition())) {
            return reader.readPayloadSequence();
        }
    }

    private void verifyStoredLayout(final MemDirectory directory) {
        final ChunkStoreFile rawStore = new ChunkStoreFile(directory, FILE_NAME,
                BLOCK_SIZE, ENCODING, List.of(new ChunkFilterCrc32Validation()));
        try (ChunkStoreReader reader = rawStore
                .openReader(rawStore.getFirstChunkStorePosition())) {
            final Chunk raw = reader.read();
            final ByteSequence compressed = raw.getPayloadSequence();
            compressedBytes = compressed.length();
            final long zstdFlag = 1L << ChunkFilter.BIT_POSITION_ZSTD_COMPRESSION;
            if ((raw.getHeader().getFlags() & zstdFlag) == 0
                    || compressedBytes >= payloadSize) {
                throw new IllegalStateException("Fixture was not compressed");
            }
            final boolean multiblock = payloadSize == 65536;
            if ((compressedBytes > BLOCK_SIZE.getPayloadSize()) != multiblock
                    || (compressed instanceof ConcatenatedByteSequence)
                            != multiblock) {
                throw new IllegalStateException("Unexpected compressed layout");
            }
        }
    }
}
