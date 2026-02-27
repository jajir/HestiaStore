package org.hestiastore.benchmark.chunkstore;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.chunkstore.ChunkStoreFile;
import org.hestiastore.index.chunkstore.ChunkStoreWriter;
import org.hestiastore.index.chunkstore.ChunkStoreWriterTx;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.directory.Directory;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures a one-shot chunk write scenario.
 * <p>
 * In simple words: every benchmark call behaves like an isolated real write.
 * It creates/open writer transaction, writes one chunk, then closes and
 * commits.
 * </p>
 * <p>
 * This benchmark includes write transaction open/close/commit overhead.
 * </p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class ChunkStoreWriteBenchmark {

    private static final DataBlockSize DATA_BLOCK_SIZE = DataBlockSize
            .ofDataBlockSize(1024);
    private static final int VERSION = 1;
    private static final String FILE_NAME = "chunk-write-benchmark";
    private static final List<ChunkFilter> ENCODING_FILTERS = List
            .of(new ChunkFilterMagicNumberWriting(),
                    new ChunkFilterCrc32Writing(), new ChunkFilterDoNothing());
    private static final List<ChunkFilter> DECODING_FILTERS = List
            .of(new ChunkFilterCrc32Validation(), new ChunkFilterDoNothing());

    @Param({ "128", "1008", "4096" })
    private int payloadSize;

    private ByteSequence payload;

    private ChunkStoreWriterTx writerTx;
    private ChunkStoreWriter writer;

    @Setup(Level.Trial)
    public void setupPayload() {
        byte[] data = new byte[payloadSize];
        for (int i = 0; i < payloadSize; i++) {
            data[i] = (byte) (i * 31 + 17);
        }
        payload = ByteSequences.wrap(data);
    }

    @Setup(Level.Invocation)
    public void setupWriter() {
        Directory directory = new MemDirectory();
        ChunkStoreFile chunkStoreFile = new ChunkStoreFile(directory, FILE_NAME,
                DATA_BLOCK_SIZE, ENCODING_FILTERS, DECODING_FILTERS);
        writerTx = chunkStoreFile.openWriteTx();
        writer = writerTx.open();
    }

    @TearDown(Level.Invocation)
    public void tearDownWriter() {
        if (writer != null) {
            writer.close();
        }
        if (writerTx != null) {
            writerTx.commit();
        }
    }

    /**
     * Writes one chunk with full transaction lifecycle per invocation.
     *
     * @return start position of written chunk
     */
    @Benchmark
    public int writeChunkTxPerInvocation() {
        return writer.writeSequence(payload, VERSION).getValue();
    }
}
