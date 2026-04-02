package org.hestiastore.benchmark.diskio.read.sequential;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.hestiastore.benchmark.diskio.DiskIoBenchmarkSupport;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FsDirectory;
import org.hestiastore.index.unsorteddatafile.UnsortedDataFile;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures one full sequential scan over a filesystem-backed unsorted data
 * file.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
public class SequentialFileReadingBenchmark {

    private static final String FILE_NAME = "read-benchmark.unsorted";
    private static final int NUMBER_OF_TESTING_PAIRS = 800_000;
    private static final TypeDescriptor<String> TYPE_DESCRIPTOR_STRING = new TypeDescriptorString();
    private static final TypeDescriptor<Long> TYPE_DESCRIPTOR_LONG = new TypeDescriptorLong();

    @Param({ "1024", "4096", "32768" })
    private int diskIoBufferSizeBytes;

    private File tempDir;
    private Directory directory;
    private UnsortedDataFile<String, Long> testFile;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = DiskIoBenchmarkSupport.createTempDir("hestia-jmh-seq-read");
        directory = new FsDirectory(tempDir);
        testFile = getDataFile(diskIoBufferSizeBytes);
        testFile.openWriterTx().execute(writer -> {
            for (int index = 0; index < NUMBER_OF_TESTING_PAIRS; index++) {
                writer.write(DiskIoBenchmarkSupport.buildSequentialKey(index),
                        DiskIoBenchmarkSupport.buildLongValue(index));
            }
        });
    }

    @Benchmark
    public long readSequentialFile() {
        long checksum = 0;
        try (EntryIterator<String, Long> pairIterator = testFile
                .openIterator()) {
            while (pairIterator.hasNext()) {
                final Entry<String, Long> entry = pairIterator.next();
                checksum += entry.getKey().length();
                checksum += entry.getValue().longValue();
            }
        }
        return checksum;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        testFile = null;
        directory = null;
        if (tempDir != null) {
            DiskIoBenchmarkSupport.deleteRecursively(tempDir);
            tempDir = null;
        }
    }

    private UnsortedDataFile<String, Long> getDataFile(final int bufferSize) {
        return UnsortedDataFile.<String, Long>builder()//
                .withDirectory(directory)//
                .withFileName(FILE_NAME)//
                .withKeyWriter(TYPE_DESCRIPTOR_STRING.getTypeWriter())//
                .withKeyReader(TYPE_DESCRIPTOR_STRING.getTypeReader())//
                .withValueWriter(TYPE_DESCRIPTOR_LONG.getTypeWriter())//
                .withValueReader(TYPE_DESCRIPTOR_LONG.getTypeReader())//
                .withDiskIoBufferSize(bufferSize)//
                .build();
    }

}
