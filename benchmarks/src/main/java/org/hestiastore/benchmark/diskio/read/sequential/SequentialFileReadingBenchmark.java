package org.hestiastore.benchmark.diskio.read.sequential;

import java.util.concurrent.TimeUnit;

import org.hestiastore.benchmark.diskio.AbstractDiskIoBenchmark;
import org.hestiastore.benchmark.diskio.DiskIoDataSupport;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
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
public class SequentialFileReadingBenchmark extends AbstractDiskIoBenchmark {

    private static final int NUMBER_OF_TESTING_PAIRS = 800_000;

    @Override
    protected String tempDirPrefix() {
        return "hestia-jmh-seq-read";
    }

    @Override
    protected String fileName() {
        return "read-benchmark.unsorted";
    }

    @Override
    protected void prepareBenchmarkData() {
        testFile.openWriterTx().execute(writer -> {
            for (int index = 0; index < NUMBER_OF_TESTING_PAIRS; index++) {
                writer.write(DiskIoDataSupport.buildSequentialKey(index),
                        DiskIoDataSupport.buildLongValue(index));
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

}
