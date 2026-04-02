package org.hestiastore.benchmark.diskio.write.sequential;

import java.util.concurrent.TimeUnit;

import org.hestiastore.benchmark.diskio.AbstractDiskIoBenchmark;
import org.hestiastore.benchmark.diskio.DiskIoDataSupport;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures the cost of rewriting one filesystem-backed unsorted data file.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
public class SequentialFileWritingBenchmark extends AbstractDiskIoBenchmark {

    private static final int NUMBER_OF_TESTING_PAIRS = 400_000;
    private String[] keys;
    private Long[] values;

    @Override
    protected String tempDirPrefix() {
        return "hestia-jmh-seq-write";
    }

    @Override
    protected String fileName() {
        return "write-benchmark.unsorted";
    }

    @Override
    protected void prepareBenchmarkData() {
        keys = new String[NUMBER_OF_TESTING_PAIRS];
        values = new Long[NUMBER_OF_TESTING_PAIRS];
        for (int index = 0; index < NUMBER_OF_TESTING_PAIRS; index++) {
            keys[index] = DiskIoDataSupport.buildSequentialKey(index);
            values[index] = DiskIoDataSupport.buildLongValue(index);
        }
    }

    @Benchmark
    public int writeSequentialFile() {
        testFile.openWriterTx().execute(writer -> {
            for (int index = 0; index < NUMBER_OF_TESTING_PAIRS; index++) {
                writer.write(keys[index], values[index]);
            }
        });
        return NUMBER_OF_TESTING_PAIRS;
    }

}
