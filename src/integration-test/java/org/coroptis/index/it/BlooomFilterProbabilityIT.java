package org.coroptis.index.it;

import java.util.Random;

import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.bloomfilter.BloomFilterWriter;
import org.hestiastore.index.bloomfilter.BloomFilterWriterTx;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verify that real bloom fileter performance met mathematical expextations.
 * 
 * At the end it's only way to somehow prove that bloom filter works.
 * 
 */
class BlooomFilterProbabilityIT {

    private static final TypeDescriptorLong STD = new TypeDescriptorLong();
    private static final String FILE_NAME = "segment-00880.bloomFilter";
    private static final Long WRITE_KEYS_IN_FILTER = 5_000_000L;
    private static final int BLOOM_FILTER_SIZE_IN_BYTES = 10_000_000;
    private static final int NUMBER_OF_HASH_FUNCTIONS = 4;

    /**
     * This number doesn't affect probability of false positive, just make it
     * more accurate
     */
    private static final Long TEST_KEYS = 10_000_000L;

    private static final Random RANDOM = new Random();
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private MemDirectory directory;

    private BloomFilter<Long> bloomFilter;

    @Test
    void test_probability() {
        long cx = 0;
        final BloomFilterWriterTx<Long> tx = bloomFilter.openWriteTx();
        try (BloomFilterWriter<Long> writer = tx.open()) {
            for (long i = 0; i < WRITE_KEYS_IN_FILTER; i++) {
                if (!writer.write(i)) {
                    cx++;
                }
            }
        }
        tx.commit();
        logger.info("Number of keys that were not stored: {}", cx);

        /**
         * Test how many random keys will be marked as stored even if it's sure
         * that wasn't.
         */
        cx = 0;
        long falsePositiveCx = 0;
        for (long i = 0; i < TEST_KEYS; i++) {
            final Long key = RANDOM.nextLong();
            boolean isProbablyStored = !bloomFilter.isNotStored(key);
            if (isProbablyStored) {
                cx++;
                if (key >= WRITE_KEYS_IN_FILTER) {
                    falsePositiveCx++;
                    bloomFilter.incrementFalsePositive();
                }
            }
        }

        double falsePositive = (double) falsePositiveCx / (double) cx;
        double falsePositive2 = (double) falsePositiveCx / (double) TEST_KEYS;
        int coutPositives = (int) Math
                .round((double) cx / (double) falsePositiveCx);
        logger.info("Number of test keys: {}", TEST_KEYS);
        logger.info("Total number od false positive: {}", cx);
        logger.info("false positive is 1 in {}", coutPositives);
        logger.info("False positive probability is {}", falsePositive);
        logger.info("False positive probability2 is {}", falsePositive2);
    }

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        bloomFilter = makeBloomFilter();
    }

    @AfterEach
    void tearDown() {
        if (bloomFilter != null) {
            bloomFilter.close();
        }
        bloomFilter = null;
        directory = null;
    }

    private BloomFilter<Long> makeBloomFilter() {
        return BloomFilter.<Long>builder()//
                .withBloomFilterFileName(FILE_NAME)//
                .withConvertorToBytes(STD.getConvertorToBytes())//
                .withDirectory(
                        directory)//
                .withIndexSizeInBytes(BLOOM_FILTER_SIZE_IN_BYTES)//
                .withNumberOfHashFunctions(NUMBER_OF_HASH_FUNCTIONS)//
                .withRelatedObjectName("segment-00323")//
                .build();
    }

}
