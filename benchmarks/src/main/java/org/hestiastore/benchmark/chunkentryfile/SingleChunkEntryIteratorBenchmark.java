package org.hestiastore.benchmark.chunkentryfile;

import java.util.concurrent.TimeUnit;

import org.hestiastore.index.Entry;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.chunkentryfile.SingleChunkEntryIterator;
import org.hestiastore.index.chunkentryfile.SingleChunkEntryWriterImpl;
import org.hestiastore.index.chunkstore.Chunk;
import org.hestiastore.index.chunkstore.ChunkHeader;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorString;
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
 * Measures iteration over entries stored in one chunk payload.
 * <p>
 * In simple words: benchmark reads all entries from one chunk repeatedly.
 * </p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class SingleChunkEntryIteratorBenchmark {

    private static final int VERSION = 1;

    @Param({ "64", "256" })
    private int entryCount;

    @Param({ "16", "64" })
    private int valueLength;

    private final TypeDescriptorInteger keyTypeDescriptor = new TypeDescriptorInteger();
    private final TypeDescriptorString valueTypeDescriptor = new TypeDescriptorString();

    private ByteSequence payload;
    private Chunk chunk;

    @Setup
    public void setupPayload() {
        final SingleChunkEntryWriterImpl<Integer, String> writer = new SingleChunkEntryWriterImpl<>(
                keyTypeDescriptor, valueTypeDescriptor);
        for (int key = 0; key < entryCount; key++) {
            writer.put(Entry.of(key, buildValue(key)));
        }
        payload = writer.closeSequence();
        chunk = Chunk.of(ChunkHeader.of(ChunkHeader.MAGIC_NUMBER, VERSION,
                payload.length(), 0L), payload);
    }

    /**
     * Iterates entries using direct payload-sequence constructor.
     *
     * @return checksum over keys and value lengths
     */
    @Benchmark
    public int iterateFromPayloadSequence() {
        int checksum = 0;
        final SingleChunkEntryIterator<Integer, String> iterator = new SingleChunkEntryIterator<>(
                payload, keyTypeDescriptor, valueTypeDescriptor);
        while (iterator.hasNext()) {
            final Entry<Integer, String> entry = iterator.next();
            checksum += entry.getKey();
            checksum += entry.getValue().length();
        }
        iterator.close();
        return checksum;
    }

    /**
     * Iterates entries using compatibility constructor that accepts
     * {@link Chunk}.
     *
     * @return checksum over keys and value lengths
     */
    @Benchmark
    public int iterateFromChunk() {
        int checksum = 0;
        final SingleChunkEntryIterator<Integer, String> iterator = new SingleChunkEntryIterator<>(
                chunk, keyTypeDescriptor, valueTypeDescriptor);
        while (iterator.hasNext()) {
            final Entry<Integer, String> entry = iterator.next();
            checksum += entry.getKey();
            checksum += entry.getValue().length();
        }
        iterator.close();
        return checksum;
    }

    private String buildValue(final int key) {
        final String prefix = Integer.toString(key) + '-';
        final int suffixLength = Math.max(0, valueLength - prefix.length());
        return prefix + "x".repeat(suffixLength);
    }
}
