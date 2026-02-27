package org.hestiastore.benchmark.chunkstore;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.chunkstore.CellStoreWriter;
import org.hestiastore.index.chunkstore.CellStoreWriterCursor;
import org.hestiastore.index.chunkstore.CellStoreWriterImpl;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.chunkstore.ChunkStoreWriter;
import org.hestiastore.index.chunkstore.ChunkStoreWriterImpl;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datablockfile.DataBlockWriter;
import org.hestiastore.index.datablockfile.DataBlockWriterImpl;
import org.hestiastore.index.directory.FileWriter;
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
 * Measures steady-state chunk writing.
 * <p>
 * In simple words: writer is opened once, then many chunks are written in a
 * row without reopening transaction for every operation.
 * </p>
 * <p>
 * This benchmark focuses on core write pipeline cost and minimizes
 * transaction/storage setup overhead.
 * </p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class ChunkStoreSteadyWriteBenchmark {

    private static final DataBlockSize DATA_BLOCK_SIZE = DataBlockSize
            .ofDataBlockSize(1024);
    private static final int VERSION = 1;
    private static final List<ChunkFilter> ENCODING_FILTERS = List
            .of(new ChunkFilterMagicNumberWriting(),
                    new ChunkFilterCrc32Writing(), new ChunkFilterDoNothing());

    @Param({ "128", "1008", "4096" })
    private int payloadSize;

    private ByteSequence payload;
    private ChunkStoreWriter writer;

    @Setup(Level.Trial)
    public void setupPayload() {
        byte[] data = new byte[payloadSize];
        for (int i = 0; i < payloadSize; i++) {
            data[i] = (byte) (i * 31 + 17);
        }
        payload = ByteSequences.wrap(data);
    }

    @Setup(Level.Iteration)
    public void setupWriter() {
        final FileWriter fileWriter = new DiscardingFileWriter();
        final DataBlockWriter dataBlockWriter = new DataBlockWriterImpl(
                fileWriter, DATA_BLOCK_SIZE);
        final CellStoreWriterCursor cursor = new CellStoreWriterCursor(
                dataBlockWriter, DATA_BLOCK_SIZE);
        final CellStoreWriter cellStoreWriter = new CellStoreWriterImpl(cursor);
        writer = new ChunkStoreWriterImpl(cellStoreWriter, ENCODING_FILTERS);
    }

    @TearDown(Level.Iteration)
    public void tearDownWriter() {
        if (writer != null) {
            writer.close();
        }
    }

    /**
     * Writes one chunk while reusing the same writer during iteration.
     *
     * @return start position of written chunk
     */
    @Benchmark
    public int writeChunkSteadyState() {
        return writer.writeSequence(payload, VERSION).getValue();
    }

    private static final class DiscardingFileWriter
            extends AbstractCloseableResource implements FileWriter {

        @Override
        public void write(final byte b) {
            // Discard benchmark output.
        }

        @Override
        public void write(final byte[] bytes) {
            // Discard benchmark output.
        }

        @Override
        protected void doClose() {
            // Nothing to release.
        }
    }
}
