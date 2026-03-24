package org.hestiastore.benchmark.sorteddatafile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.directory.FsDirectory;
import org.hestiastore.index.sorteddatafile.DiffKeyReader;
import org.hestiastore.index.sorteddatafile.SortedDataFile;
import org.hestiastore.index.sorteddatafile.SortedDataFileWriterTx;
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
 * Measures sequential key decoding through {@link DiffKeyReader}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class DiffKeyReaderBenchmark {

    private static final String FILE_NAME = "diff-key-reader-benchmark";
    private static final TypeDescriptorString TYPE_DESCRIPTOR = new TypeDescriptorString();
    private static final Path JMH_TEMP_ROOT = Path.of("target", "jmh-temp");

    @Param({ "1024", "8192" })
    private int entryCount;

    @Param({ "24", "48" })
    private int keyLength;

    @Param({ "16", "64" })
    private int valueLength;

    private Path tempDir;
    private FsDirectory directory;
    private SortedDataFile<String, String> dataFile;

    private FileReader fileReader;
    private DiffKeyReader<String> keyReader;
    private TypeReader<String> valueReader;

    @Setup(Level.Trial)
    public void setupDataFile() throws IOException {
        Files.createDirectories(JMH_TEMP_ROOT);
        tempDir = Files.createTempDirectory(JMH_TEMP_ROOT,
                "hestia-jmh-diffkey-");
        directory = new FsDirectory(tempDir.toFile());
        dataFile = SortedDataFile.<String, String>builder()//
                .withKeyTypeDescriptor(TYPE_DESCRIPTOR)//
                .withValueTypeDescriptor(TYPE_DESCRIPTOR)//
                .withDirectory(directory)//
                .withFileName(FILE_NAME)//
                .build();
        writeEntries();
    }

    @Setup(Level.Iteration)
    public void openReaders() {
        fileReader = directory.getFileReader(FILE_NAME);
        keyReader = new DiffKeyReader<>(TYPE_DESCRIPTOR.getTypeDecoder());
        valueReader = TYPE_DESCRIPTOR.getTypeReader();
    }

    @TearDown(Level.Iteration)
    public void closeReaders() {
        if (fileReader != null) {
            fileReader.close();
            fileReader = null;
        }
        keyReader = null;
        valueReader = null;
    }

    @TearDown(Level.Trial)
    public void tearDownDataFile() throws IOException {
        closeReaders();
        dataFile = null;
        directory = null;
        if (tempDir != null) {
            deleteRecursively(tempDir.toFile());
            tempDir = null;
        }
    }

    /**
     * Reads one diff-encoded key and consumes the paired value to keep the file
     * cursor aligned.
     *
     * @return checksum based on decoded key and value lengths
     */
    @Benchmark
    public int readNextKey() {
        String key = keyReader.read(fileReader);
        if (key == null) {
            closeReaders();
            openReaders();
            key = keyReader.read(fileReader);
        }
        final String value = valueReader.read(fileReader);
        return key.length() + value.length();
    }

    private void writeEntries() {
        final SortedDataFileWriterTx<String, String> tx = dataFile
                .openWriterTx();
        try (var writer = tx.open()) {
            for (int key = 0; key < entryCount; key++) {
                writer.write(Entry.of(buildKey(key), buildValue(key)));
            }
        }
        tx.commit();
    }

    private String buildKey(final int key) {
        final String suffix = String.format("%010d", Integer.valueOf(key));
        final int prefixLength = Math.max(0, keyLength - suffix.length() - 1);
        return "k".repeat(prefixLength) + '-' + suffix;
    }

    private String buildValue(final int key) {
        final String prefix = "v" + key + '-';
        final int suffixLength = Math.max(0, valueLength - prefix.length());
        return prefix + "y".repeat(suffixLength);
    }

    private void deleteRecursively(final File file) throws IOException {
        if (file == null || !file.exists()) {
            return;
        }
        final File[] children = file.listFiles();
        if (children != null) {
            java.util.Arrays.sort(children,
                    Comparator.comparing(File::getAbsolutePath).reversed());
            for (final File child : children) {
                deleteRecursively(child);
            }
        }
        if (!file.delete()) {
            throw new IOException(
                    "Unable to delete benchmark temp path: "
                            + file.getAbsolutePath());
        }
    }
}
