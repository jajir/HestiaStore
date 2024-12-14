package com.coroptis.index.benchmark;

import java.io.File;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairWriter;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorLong;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.Directory.Access;
import com.coroptis.index.directory.FsDirectory;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;

@BenchmarkMode(Mode.AverageTime) // Measures the average time per operation
@OutputTimeUnit(TimeUnit.MILLISECONDS) // Results in milliseconds
@State(Scope.Thread) // Each thread gets its own state
public class FileReadingBenchmark {

    private final Logger logger = LoggerFactory
            .getLogger(FileReadingBenchmark.class);
    private final static String PROPERTY_DIRECTORY = "dir";
    private final static String FILE_NAME = "test.unsorted";
    private final static Random RANDOM = new Random();
    private final static int RANDOM_STRING_LENGTH = 20;
    private final String CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private final static int NUMBER_OF_TESTING_PAIRS = 10_000;
    private final static TypeDescriptor<String> TYPE_DESCRIPTOR_STRING = new TypeDescriptorString();
    private final static TypeDescriptor<Long> TYPE_DESCRIPTOR_LONG = new TypeDescriptorLong();

    private String directoryFileName;
    private UnsortedDataFile<String, Long> testFile;
    private Directory directory;

    @Setup
    public void setup() {
        directoryFileName = System.getProperty(PROPERTY_DIRECTORY);
        logger.debug("Property 'dir' is '" + directoryFileName + "'");
        if (directoryFileName == null || directoryFileName.isEmpty()) {
            throw new IllegalStateException("Property 'dir' is not set");
        }
        directory = new FsDirectory(new File(directoryFileName));

        testFile = UnsortedDataFile.<String, Long>builder()//
                .withDirectory(directory)//
                .withFileName(FILE_NAME)//
                .withKeyWriter(TYPE_DESCRIPTOR_STRING.getTypeWriter())//
                .withKeyReader(TYPE_DESCRIPTOR_STRING.getTypeReader())//
                .withValueWriter(TYPE_DESCRIPTOR_LONG.getTypeWriter())//
                .withValueReader(TYPE_DESCRIPTOR_LONG.getTypeReader())//
                .build();

        // prepare data
        try (PairWriter<String, Long> pairWriter = testFile
                .openWriter(Access.OVERWRITE)) {
            for (int i = 0; i < NUMBER_OF_TESTING_PAIRS; i++) {
                pairWriter.put(generateRandomString(), RANDOM.nextLong());
            }
        }
    }

    @Benchmark
    public String testReadDataWithBuffer1KB() {
        return testRound(1024);
    }

    @Benchmark
    public String testReadDataWithBuffer2KB() {
        return testRound(2*1024);
    }

    @Benchmark
    public String testReadDataWithBuffer4KB() {
        return testRound(4*1024);
    }

    @Benchmark
    public String testReadDataWithBuffer8KB() {
        return testRound(8*1024);
    }

    @Benchmark
    public String testReadDataWithBuffer16KB() {
        return testRound(16*1024);
    }

    private String testRound(final int bufferSize) {
        long result = 0;
        try (PairIterator<String, Long> pairIterator = testFile
                .openIterator(bufferSize)) {
            while (pairIterator.hasNext()) {
                final Pair<String, Long> pair = pairIterator.next();
                if (pair == null) {
                    throw new IllegalStateException("Pair is null");
                }
                if (pair.getKey() == null) {
                    throw new IllegalStateException("Key is null");
                }
                if (pair.getValue() == null) {
                    throw new IllegalStateException("Value is null");
                }
                result++;
            }
        }
        return String.valueOf(result);
    }

    private String generateRandomString() {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < RANDOM_STRING_LENGTH; i++) {
            int randomIndex = RANDOM.nextInt(CHARACTERS.length());
            sb.append(CHARACTERS.charAt(randomIndex));
        }
        return sb.toString();
    }

}
