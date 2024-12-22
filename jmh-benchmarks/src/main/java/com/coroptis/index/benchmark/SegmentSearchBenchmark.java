package com.coroptis.index.benchmark;

import java.io.File;
import java.util.Random;
import java.util.concurrent.TimeUnit;

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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.PairWriter;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorLong;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FsDirectory;
import com.coroptis.index.segment.Segment;
import com.coroptis.index.segment.SegmentBuilder;
import com.coroptis.index.segment.SegmentId;

/**
 * Test will create segment with String and Long as key value pairs.
 * 
 * Test data will be sequence, 10% of entry data will be randomly ommited from
 * storing. Test will randomly read data from segment.
 */
@BenchmarkMode(Mode.AverageTime) // Measures the average time per operation
@OutputTimeUnit(TimeUnit.MILLISECONDS) // Results in milliseconds
@State(Scope.Thread) // Each thread gets its own state
@Warmup(iterations = 0, time = 1) // 0 warm-up iterations
@Measurement(iterations = 4, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1) // Use 1 fork (JVM instance)
@Threads(1)
public class SegmentSearchBenchmark {

    private final Logger logger = LoggerFactory
            .getLogger(SequentialFileReadingBenchmark.class);
    private final static String PROPERTY_DIRECTORY = "dir";
    private final SegmentId SEGMENT_ID = SegmentId.of(29);
    private final static Random RANDOM = new Random();
    private final static DataProvider dataProvider = new DataProvider();
    private final static int NUMBER_OF_TESTING_PAIRS = 2_000_000;
    private final static int NUMBER_OF_TESTING_SEARCH_OPERATIONS = 1_000;
    private final static TypeDescriptor<String> TYPE_DESCRIPTOR_STRING = new TypeDescriptorString();
    private final static TypeDescriptor<Long> TYPE_DESCRIPTOR_LONG = new TypeDescriptorLong();

    private String directoryFileName;
    private Directory directory;
    private Segment<String, Long> segment;

    @Param({ "1", "2", "4", "16", "32" })
    private int diskIoBufferSize;

    @Setup
    public void setup() {
        directoryFileName = System.getProperty(PROPERTY_DIRECTORY);
        logger.debug("Property 'dir' is '" + directoryFileName + "'");
        if (directoryFileName == null || directoryFileName.isEmpty()) {
            throw new IllegalStateException("Property 'dir' is not set");
        }
        directory = new FsDirectory(new File(directoryFileName));

        final Segment<String, Long> segment = getCommonBuilder()// get default
                // builder
                .withMaxNumberOfKeysInSegmentCache(1000)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(100_000)//
                .build();

        if (segment.getNumberOfKeys() != NUMBER_OF_TESTING_PAIRS) {
            System.out.println("main setup - rebuilding, it's "
                    + segment.getNumberOfKeys());
            try (PairWriter<String, Long> pairWriter = segment.openWriter()) {
                for (int i = 0; i < NUMBER_OF_TESTING_PAIRS; i++) {
                    if (RANDOM.nextInt(10) != 0) {
                        pairWriter.put(dataProvider.generateSequenceString(i),
                                RANDOM.nextLong());
                    }
                }
            }
            segment.forceCompact();
        }

        segment.close();
    }

    /**
     * Should be run before each iteration (measurement).
     */
    @Setup(Level.Iteration)
    public void setupIteration() {
        int bufferSize = 1024 * diskIoBufferSize;
        segment = getCommonBuilder()// default builder
                .withDiskIoBufferSize(bufferSize)//
                .build();
    }

    @Benchmark
    public String test_search() {
        long result = 0;
    
        // prepare data
        for (int i = 0; i < NUMBER_OF_TESTING_SEARCH_OPERATIONS; i++) {
            final String key = dataProvider.generateSequenceString(
                    RANDOM.nextInt(NUMBER_OF_TESTING_PAIRS));
            final Long value = segment.get(key);
            if (value != null) {
                result += value;
            }
        }
        return String.valueOf(result);
    }

    private SegmentBuilder<String, Long> getCommonBuilder() {
        return Segment.<String, Long>builder()//
                .withDirectory(directory)//
                .withId(SEGMENT_ID)//
                .withKeyTypeDescriptor(TYPE_DESCRIPTOR_STRING)//
                .withValueTypeDescriptor(TYPE_DESCRIPTOR_LONG)//
                .withMaxNumberOfKeysInSegmentCache(3)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(100)//
                .withMaxNumberOfKeysInIndexPage(100)//
                .withBloomFilterIndexSizeInBytes(0);// disable bloom filter
    }

}
