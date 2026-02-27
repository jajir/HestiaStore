package org.hestiastore.benchmark.datablockfile;

import java.util.concurrent.TimeUnit;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.datablockfile.DataBlock;
import org.hestiastore.index.datablockfile.DataBlockByteReader;
import org.hestiastore.index.datablockfile.DataBlockByteReaderImpl;
import org.hestiastore.index.datablockfile.DataBlockReader;
import org.hestiastore.index.datablockfile.DataBlockSize;
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
 * Measures steady-state reads from {@link DataBlockByteReader}.
 * <p>
 * In simple words: this benchmark keeps reading fixed-size byte slices from an
 * endless stream of data-block payloads.
 * </p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class DataBlockByteReaderBenchmark {

    private static final DataBlockSize DATA_BLOCK_SIZE = DataBlockSize
            .ofDataBlockSize(1024);
    private static final int PAYLOAD_SIZE = DATA_BLOCK_SIZE.getPayloadSize();

    @Param({ "128", "1008", "2016", "4096" })
    private int readLength;

    private DataBlockByteReader reader;

    @Setup(Level.Iteration)
    public void setupReader() {
        final DataBlockReader dataBlockReader = new RepeatingDataBlockReader();
        reader = new DataBlockByteReaderImpl(dataBlockReader, DATA_BLOCK_SIZE,
                0);
    }

    @TearDown(Level.Iteration)
    public void tearDownReader() {
        if (reader != null) {
            reader.close();
        }
    }

    /**
     * Reads exactly {@code readLength} bytes and returns one byte to keep the
     * payload alive in benchmark results.
     *
     * @return unsigned last-byte value from the read sequence
     */
    @Benchmark
    public int readExactlySequenceSteadyState() {
        final ByteSequence bytes = reader.readExactlySequence(readLength);
        return bytes.getByte(bytes.length() - 1) & 0xFF;
    }

    private static final class RepeatingDataBlockReader
            extends AbstractCloseableResource implements DataBlockReader {

        private static final int PAYLOAD_COUNT = 16;

        private final ByteSequence[] payloads;
        private int index = 0;

        RepeatingDataBlockReader() {
            this.payloads = new ByteSequence[PAYLOAD_COUNT];
            for (int payloadIndex = 0; payloadIndex < PAYLOAD_COUNT; payloadIndex++) {
                byte[] payload = new byte[PAYLOAD_SIZE];
                for (int i = 0; i < payload.length; i++) {
                    payload[i] = (byte) (payloadIndex * 31 + i * 17);
                }
                payloads[payloadIndex] = ByteSequences.wrap(payload);
            }
        }

        @Override
        public DataBlock read() {
            throw new UnsupportedOperationException(
                    "Use readPayloadSequence() in this benchmark reader.");
        }

        @Override
        public ByteSequence readPayloadSequence() {
            final ByteSequence out = payloads[index];
            index++;
            if (index == payloads.length) {
                index = 0;
            }
            return out;
        }

        @Override
        protected void doClose() {
            // Nothing to release.
        }
    }
}
