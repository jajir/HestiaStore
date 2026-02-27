package org.hestiastore.benchmark.sorteddatafile;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.directory.FileWriter;
import org.hestiastore.index.sorteddatafile.DiffKeyWriter;
import org.hestiastore.index.sorteddatafile.SortedDataFileWriter;
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
 * Measures writing sorted key-value entries into a data file.
 * <p>
 * In simple words: benchmark writes many ordered entries and compares current
 * direct diff-key writing against legacy array-based diff-key writing.
 * </p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class SortedDataFileWriterBenchmark {

    @Param({ "128", "1024" })
    private int entryCount;

    @Param({ "16", "64" })
    private int valueLength;

    private final TypeDescriptorInteger keyTypeDescriptor = new TypeDescriptorInteger();
    private final TypeDescriptorString valueTypeDescriptor = new TypeDescriptorString();

    private List<Entry<Integer, String>> entries;

    @Setup(Level.Trial)
    public void setupEntries() {
        final List<Entry<Integer, String>> generated = new ArrayList<>(
                entryCount);
        for (int key = 0; key < entryCount; key++) {
            generated.add(Entry.of(key, buildValue(key)));
        }
        entries = generated;
    }

    /**
     * Writes all entries using current {@link SortedDataFileWriter} path.
     *
     * @return number of bytes written
     */
    @Benchmark
    public long writeEntriesCurrentPath() {
        final CountingDiscardingFileWriter fileWriter = new CountingDiscardingFileWriter();
        try (SortedDataFileWriter<Integer, String> writer = new SortedDataFileWriter<>(
                valueTypeDescriptor.getTypeWriter(), fileWriter,
                keyTypeDescriptor)) {
            for (Entry<Integer, String> entry : entries) {
                writer.write(entry);
            }
        }
        return fileWriter.writtenBytes();
    }

    /**
     * Writes all entries using legacy diff-key array path.
     *
     * @return number of bytes written
     */
    @Benchmark
    public long writeEntriesLegacyArrayPath() {
        final CountingDiscardingFileWriter fileWriter = new CountingDiscardingFileWriter();
        try (LegacySortedDataWriter<Integer, String> writer = new LegacySortedDataWriter<>(
                valueTypeDescriptor.getTypeWriter(), fileWriter,
                keyTypeDescriptor)) {
            for (Entry<Integer, String> entry : entries) {
                writer.write(entry);
            }
        }
        return fileWriter.writtenBytes();
    }

    private String buildValue(final int key) {
        final String prefix = Integer.toString(key) + '-';
        final int suffixLength = Math.max(0, valueLength - prefix.length());
        return prefix + "x".repeat(suffixLength);
    }

    private static final class CountingDiscardingFileWriter
            extends AbstractCloseableResource implements FileWriter {

        private long writtenBytes = 0;

        @Override
        public void write(final byte b) {
            writtenBytes++;
        }

        @Override
        public void write(final byte[] bytes) {
            writtenBytes += bytes.length;
        }

        @Override
        public void write(final byte[] bytes, final int offset,
                final int length) {
            writtenBytes += length;
        }

        long writtenBytes() {
            return writtenBytes;
        }

        @Override
        protected void doClose() {
            // Nothing to close.
        }
    }

    @SuppressWarnings("deprecation")
    private static final class LegacySortedDataWriter<K, V>
            extends AbstractCloseableResource implements EntryWriter<K, V> {

        private final TypeWriter<V> valueWriter;
        private final FileWriter fileWriter;
        private final DiffKeyWriter<K> diffKeyWriter;

        private LegacySortedDataWriter(final TypeWriter<V> valueWriter,
                final FileWriter fileWriter,
                final TypeDescriptor<K> keyTypeDescriptor) {
            this.valueWriter = Vldtn.requireNonNull(valueWriter, "valueWriter");
            this.fileWriter = Vldtn.requireNonNull(fileWriter, "fileWriter");
            final TypeDescriptor<K> keyType = Vldtn
                    .requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
            this.diffKeyWriter = new DiffKeyWriter<>(
                    Vldtn.requireNonNull(keyType.getTypeEncoder(),
                            "keyTypeEncoder"),
                    Vldtn.requireNonNull(keyType.getComparator(),
                            "keyComparator"));
        }

        @Override
        public void write(final Entry<K, V> entry) {
            Vldtn.requireNonNull(entry, "entry");
            Vldtn.requireNonNull(entry.getKey(), "key");
            Vldtn.requireNonNull(entry.getValue(), "value");
            final byte[] diffKey = diffKeyWriter.write(entry.getKey());
            fileWriter.write(diffKey);
            valueWriter.write(fileWriter, entry.getValue());
        }

        @Override
        protected void doClose() {
            fileWriter.close();
        }
    }
}
