package org.hestiastore.benchmark.diskio;

import java.io.File;
import java.io.IOException;

import org.hestiastore.benchmark.BenchmarkFileSupport;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FsDirectory;
import org.hestiastore.index.unsorteddatafile.UnsortedDataFile;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public abstract class AbstractDiskIoBenchmark {

    protected static final TypeDescriptor<String> TYPE_DESCRIPTOR_STRING = new TypeDescriptorString();
    protected static final TypeDescriptor<Long> TYPE_DESCRIPTOR_LONG = new TypeDescriptorLong();

    @Param({ "1024", "4096", "32768" })
    protected int diskIoBufferSizeBytes;

    protected Directory directory;
    protected UnsortedDataFile<String, Long> testFile;

    private File tempDir;

    @Setup(Level.Trial)
    public final void setUpBenchmark() throws IOException {
        tempDir = BenchmarkFileSupport.createTempDir(tempDirPrefix());
        directory = new FsDirectory(tempDir);
        testFile = buildDataFile(fileName(), diskIoBufferSizeBytes);
        prepareBenchmarkData();
    }

    @TearDown(Level.Trial)
    public final void tearDownBenchmark() {
        testFile = null;
        directory = null;
        if (tempDir != null) {
            BenchmarkFileSupport.deleteRecursively(tempDir);
            tempDir = null;
        }
    }

    protected abstract String tempDirPrefix();

    protected abstract String fileName();

    protected void prepareBenchmarkData() {
        // Default no-op.
    }

    protected final UnsortedDataFile<String, Long> buildDataFile(
            final String fileName, final int bufferSize) {
        return UnsortedDataFile.<String, Long>builder()//
                .withDirectory(directory)//
                .withFileName(fileName)//
                .withKeyWriter(TYPE_DESCRIPTOR_STRING.getTypeWriter())//
                .withKeyReader(TYPE_DESCRIPTOR_STRING.getTypeReader())//
                .withValueWriter(TYPE_DESCRIPTOR_LONG.getTypeWriter())//
                .withValueReader(TYPE_DESCRIPTOR_LONG.getTypeReader())//
                .withDiskIoBufferSize(bufferSize)//
                .build();
    }
}
